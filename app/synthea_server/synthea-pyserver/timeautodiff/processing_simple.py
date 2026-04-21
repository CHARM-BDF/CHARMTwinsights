# Vendored timeautodiff package
# Source: https://github.com/mahmoudibrahim98/icu-autodiff
# File: timeautodiff/processing_simple.py

import torch
import numpy as np
from collections import Counter
from torch.utils.data import Dataset
import pandas as pd
from sklearn.preprocessing import StandardScaler
import os
import torch.nn.functional as F

__all__ = ['StandardScaler','CustomNormalizer', 'LabelEncoder', 'FreqLabelEncoder', 'DataFrameParser', 'SingleDataset']

device = 'cuda' if torch.cuda.is_available() else 'cpu'

class CustomNormalizer(object):
    def __init__(self):
        self.max = None
        self.min = None

    def fit(self, x):
        self.max, self.min = np.max(x), np.min(x)
        return self

    def transform(self, x):
        standardized = (np.array(x) - self.min) / (self.max - self.min + 1e-7)
        imputed = np.nan_to_num(standardized)
        return imputed

    def fit_transform(self, x):
        self.fit(x)
        return self.transform(x)

    def fit_invert(self, x, encoded_col):
        self.max, self.min = np.max(x), np.min(x)
        return encoded_col * (self.max - self.min + 1e-7) + self.min

class LabelEncoder(object):
    def __init__(self):
        self.mapping = dict()

    def __len__(self):
        return len(self.mapping)

    def fit(self, x):
        np.random.seed(200)
        self.mapping = {v: i for i, v in enumerate(set(x))}
        return self

    def fit_bin_int(self, x):
        np.random.seed(200)
        self.bin_int_encoder = x
        return self

    def fit_transform(self, x):
        np.random.seed(200)
        return np.array(list(map(self.mapping.__getitem__, x)))

    def fit_int_transform(self, x):
        np.random.seed(200)
        return np.array(x)

    def fit_invert(self, x, encoded_col):
        np.random.seed(200)
        self.mapping = {v: i for i, v in enumerate(set(x))}
        inverse_mapping = {v: k for k, v in self.mapping.items()}
        return np.array(list(inverse_mapping[i] for i in encoded_col))

class FreqLabelEncoder(object):
    ''' A composition of label encoding and frequency encoding. Not reversible. '''
    def __init__(self):
        self.freq_counts = None

    def __len__(self):
        return len(self.lbl_encoder)

    def fit(self, x):
        self.freq_counts = Counter(x)
        self.lbl_encoder = LabelEncoder().fit(self.freq_counts.values())
        return self

    def transform(self, x):
        freq_encoded = np.array(list(map(self.freq_counts.__getitem__, x)))
        lbl_encoded = self.lbl_encoder.transform(freq_encoded)
        return lbl_encoded

    def fit_transform(self, x):
        self.fit(x)
        return self.transform(x)


class DataFrameParser(object):
    """ Transform dataframe to numpy array for modeling. Not a reversible process.
        It will reshuffle the columns according to datatype: binary->categorical->numerical
    """
    def __init__(self, max_cardinality=25, numerical_processing = 'normalize'):
        self.max_cardinality = max_cardinality
        self.binary_columns = list()
        self.categorical_columns = list()
        self._cards = list()
        self.numerical_columns = list()
        self.need_freq_encoding = set()
        self.need_int_encoding = list()
        self.need_bin_int = list()
        if numerical_processing == 'normalize':
            self.NumericalProcessor = CustomNormalizer()
        elif numerical_processing == 'standardize':
            self.NumericalProcessor = StandardScaler()
        self.numerical_processing = numerical_processing

    def fit(self, dataframe, threshold, fit_encoders = True):
        self.new_dataframe = dataframe.copy()
        self._original_order = self.new_dataframe.columns.tolist()
        self._original_column_to_dtype = column_to_dtype = self.new_dataframe.dtypes.to_dict()

        for column, datatype in column_to_dtype.items():
            if datatype in ['O', '<U32','bool']:
                cardinality = self.new_dataframe[column].nunique(dropna=False)
                if cardinality <= 2:
                    self.binary_columns.append(column)
                else:
                    self.categorical_columns.append(column)

            elif np.issubdtype(datatype, np.integer) and self.new_dataframe[column].nunique() <= 2:
                self.binary_columns.append(column)
                self.need_bin_int.append(column)

            elif (np.issubdtype(datatype, np.float64) or np.issubdtype(datatype, np.float32)) and self.new_dataframe[column].nunique() <= 2:
                self.binary_columns.append(column)
                self.need_bin_int.append(column)

            elif np.issubdtype(datatype, np.integer) and self.new_dataframe[column].nunique() <= 25 \
                    and self.new_dataframe[column].nunique() >= 3:
                self.categorical_columns.append(column)
                self.need_int_encoding.append(column)

            elif (np.issubdtype(datatype, np.float64) or np.issubdtype(datatype, np.float32)) and self.new_dataframe[column].nunique() <= 25 \
                    and self.new_dataframe[column].nunique() >= 3:
                self.categorical_columns.append(column)
                self.need_int_encoding.append(column)

            else:
                self.numerical_columns.append(column)   
                counts = self.new_dataframe[column].value_counts()
                repeated_entries = counts[counts > threshold * len(self.new_dataframe[column])].index.tolist()

                if len(repeated_entries) == 1:
                    new_column_name = 'Binary_' + column
                    self.binary_columns.append('Binary_' + column)
                    self.need_bin_int.append('Binary_' + column)
                    self.new_dataframe[new_column_name] = \
                        self.new_dataframe[column].apply(lambda x: repeated_entries.index(x) + 1 if x in repeated_entries else 0)
                    self.new_dataframe[new_column_name] = self.new_dataframe[new_column_name].astype(int)

                if len(repeated_entries) >= 2 and len(repeated_entries) <= 25:
                    new_column_name = 'Cate_' + column   
                    self.categorical_columns.append('Cate_' + column)
                    self.need_int_encoding.append('Cate_' + column)
                    self.new_dataframe[new_column_name] = \
                        self.new_dataframe[column].apply(lambda x: repeated_entries.index(x) + 1 if x in repeated_entries else 0)
                    self.new_dataframe[new_column_name] = self.new_dataframe[new_column_name].astype(int)

        self._column_order = self.binary_columns + self.categorical_columns + self.numerical_columns
        encoders = dict()

        if fit_encoders:
            self.fit_encoders = True
            for column in self.binary_columns:
                if self.new_dataframe[column].dtype == int:
                    encoders[column] = LabelEncoder().fit_bin_int(self.new_dataframe[column].astype(int))
                else:
                    encoders[column] = LabelEncoder().fit(self.new_dataframe[column].astype(str))

            for column in self.categorical_columns:
                if column in self.need_freq_encoding:
                    encoders[column] = FreqLabelEncoder().fit(self.new_dataframe[column].astype(str))
                elif column in self.need_int_encoding:
                    encoders[column] = LabelEncoder().fit(self.new_dataframe[column].astype(int))
                else:
                    encoders[column] = LabelEncoder().fit(self.new_dataframe[column].astype(str))
                self._cards.append(len(encoders[column]))

            for column in self.numerical_columns:
                encoders[column] = self.NumericalProcessor.fit(self.new_dataframe[column].astype(float).to_frame())

            self._embeds = [int(min(600, 1.6 * card ** .5)) for card in self._cards]
            self.encoders = encoders
            self.new_df = dataframe
        else:
            for column in self.categorical_columns:
                self._cards.append(self.new_dataframe[column].astype(str).nunique())

        return self

    def transform(self):
        if self.fit_encoders:
            df = self.new_dataframe[self._column_order].copy()
            for column, encoder in self.encoders.items():
                if column in self.numerical_columns:
                    df[column] = encoder.fit_transform(df[column].to_frame())
                elif column in self.need_int_encoding:
                    df[column] = encoder.fit_transform(df[column].astype(int))
                elif column in self.need_bin_int:
                    df[column] = encoder.fit_int_transform(df[column].astype(int))
                else:
                    df[column] = encoder.fit_transform(df[column].astype(str))
            df.columns = self._column_order
        else:
            print('Encoders not fitted')
        return df.values

    @property
    def n_bins(self): return len(self.binary_columns)

    @property
    def n_cats(self): return len(self.categorical_columns)

    @property
    def n_nums(self): return len(self.numerical_columns)

    @property
    def cards(self): return self._cards

    @property
    def embeds(self): return self._embeds

    def datatype_info(self): return {'n_bins': self.n_bins, 'n_cats': self.n_cats, 'n_nums': self.n_nums, 'cards': self._cards}

    def column_name(self): return self._column_order


def convert_to_tensor(gen_output, data_size, seq_len, datatype_info):
    def sigmoid_threshold(logits):
        sigmoid_output = torch.sigmoid(logits).to(device)
        threshold_output = torch.where(sigmoid_output > 0.5, torch.tensor(1).to(device), torch.tensor(0).to(device)).to(device)
        return threshold_output

    def softmax_with_max(predictions):
        probabilities = F.softmax(predictions.to(device), dim=2).to(device)
        max_indices = torch.argmax(probabilities.to(device), dim=2).to(device)
        return max_indices

    n_bins = datatype_info['n_bins']
    n_cats = datatype_info['n_cats']
    n_nums = datatype_info['n_nums']
    cards = datatype_info['cards']

    synth_data = torch.tensor([]).to(device)

    if n_bins != 0:
        bin_tensor = torch.empty(data_size, seq_len, n_bins).to(device)
        for idx in range(n_bins):
            bin_tensor[:,:,idx] = sigmoid_threshold(gen_output['bins'][:,:,idx].detach()).to(torch.int64)
        synth_data = bin_tensor

    if len(cards) != 0:
        cat_tensor = torch.empty(data_size, seq_len, n_cats).to(device)
        for idx in range(len(cards)):
            cat_tensor[:,:,idx] = softmax_with_max(gen_output['cats'][idx].detach()).to(torch.int64).to(device)
        synth_data = torch.cat((synth_data, cat_tensor),dim=2)

    if n_nums != 0:
        num_tensor = torch.empty(data_size, seq_len, n_nums).to(device)
        num_tensor = gen_output['nums'].detach().to(device)
        synth_data = torch.cat((synth_data, num_tensor),dim=2).to(device)

    return synth_data


def cyclical_encode_hourly(hour_duration, N, hour_period=24, day_period=365):
    hours = np.arange(hour_duration)
    days = np.floor_divide(np.arange(hour_duration), 24)
    hour_sin = np.sin(hours / hour_period * 2 * np.pi)
    hour_cos = np.cos(hours / hour_period * 2 * np.pi)
    day_sin = np.sin(days / day_period * 2 * np.pi)
    day_cos = np.cos(days / day_period * 2 * np.pi)
    time_info = np.stack((day_sin, day_cos, hour_sin, hour_cos,), axis=-1)
    time_info_transformed = np.expand_dims(time_info, axis=0)
    return np.repeat(time_info_transformed, N, axis=0)


def normalize_and_reshape(X):
    X = np.transpose(X, (0, 2, 1))
    reshaped_X = X.reshape(-1, X.shape[2])
    normalizer = CustomNormalizer()
    normalized_data = np.apply_along_axis(normalizer.fit_transform, 0, reshaped_X)
    reshaped_normalized_data = normalized_data.reshape(X.shape)
    return reshaped_normalized_data


def process_data_for_synthesizer(X, y, c, selected_features):
    patient_length = X.shape[0]
    seq_len = X.shape[2]
    X_transforemd = normalize_and_reshape(X)
    X_selected = X_transforemd[:,:,selected_features]
    time = cyclical_encode_hourly(seq_len, patient_length)
    Y_data = y[:, np.newaxis, np.newaxis]
    Y_data_reshaped = np.tile(Y_data, (1, seq_len, 1))
    if c.size > 0:
        static_cond = np.tile(c[:, np.newaxis, :], (1, seq_len, 1))
    return torch.tensor(X_selected), torch.tensor(Y_data_reshaped), torch.tensor(static_cond), torch.tensor(time)
