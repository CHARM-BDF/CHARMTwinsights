# Vendored timeautodiff package
# Source: https://github.com/mahmoudibrahim98/icu-autodiff
# File: timeautodiff/timeautodiff_v4_efficient_simple.py

import torch
import torch.nn as nn
import torch.nn.functional as F
import math
import pandas as pd
import numpy as np
from typing import List, Callable

from . import processing_simple as processing

device = 'cuda' if torch.cuda.is_available() else 'cpu'

# Diffusion parameters
diffusion_steps = 100

def get_betas(steps):
    beta_start, beta_end = 1e-4, 0.2
    diffusion_ind = torch.linspace(0, 1, steps).to(device)
    return beta_start * (1 - diffusion_ind) + beta_end * diffusion_ind

betas = get_betas(diffusion_steps)
alphas = torch.cumprod(1 - betas, dim=0)


class Embedding_data_auto(nn.Module):
    def __init__(self, input_size, emb_dim, n_bins, n_cats, n_nums, cards):
        super().__init__()
        self.input_size = input_size
        self.emb_dim = emb_dim
        self.n_bins = n_bins
        self.n_cats = n_cats
        self.n_nums = n_nums
        self.cards = cards
        self.n_disc = self.n_bins + self.n_cats
        self.num_categorical_list = [2]*self.n_bins + self.cards

        if self.n_disc != 0:
            self.embeddings_list = nn.ModuleList([
                nn.Embedding(num_categories, emb_dim)
                for num_categories in self.num_categorical_list
            ])

        if self.n_nums != 0:
            self.mlp_nums = nn.Sequential(
                nn.Linear(16 * n_nums, 16 * n_nums),
                nn.SiLU(),
                nn.Linear(16 * n_nums, 16 * n_nums)
            )

        self.mlp_output = nn.Sequential(
            nn.Linear(emb_dim * self.n_disc + 16 * n_nums, emb_dim),
            nn.ReLU(),
            nn.Linear(emb_dim, input_size)
        )

    def process_chunk(self, x, chunk_size=32):
        B, L, _ = x.shape
        dev = x.device
        x_emb_chunks = []

        for i in range(0, B, chunk_size):
            chunk = x[i:i+chunk_size].to(dev)
            x_disc = chunk[:,:,0:self.n_disc].long()
            x_nums = chunk[:,:,self.n_disc:self.n_disc+self.n_nums]

            if self.n_disc != 0:
                emb_list = []
                for j, embedding in enumerate(self.embeddings_list):
                    emb = embedding(x_disc[:,:,j])
                    emb_list.append(emb)
                x_disc_emb = torch.cat(emb_list, dim=2)
                del emb_list
            else:
                x_disc_emb = torch.tensor([], device=dev)

            if self.n_nums != 0:
                angles = 2**torch.arange(8, device=dev).float() * math.pi * x_nums.unsqueeze(-1)
                sines = torch.sin(angles)
                cosines = torch.cos(angles)

                trig_values = torch.cat([
                    sines.reshape(*sines.shape[:-2], -1),
                    cosines.reshape(*cosines.shape[:-2], -1)
                ], dim=-1)
                del sines, cosines, angles

                x_nums_emb = self.mlp_nums(trig_values)
                x_emb = torch.cat([x_disc_emb, x_nums_emb], dim=2)
                del trig_values, x_nums_emb
            else:
                x_emb = x_disc_emb

            x_emb = self.mlp_output(x_emb)
            x_emb_chunks.append(x_emb)

            del x_disc, x_nums, x_disc_emb
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        return torch.cat(x_emb_chunks, dim=0)

    def forward(self, x):
        self.to(x.device)
        return self.process_chunk(x)


class Embedding_data_diff(nn.Module):
    def __init__(self, input_size, emb_dim, n_bins, n_cats, n_nums, cards):
        super().__init__()
        self.n_bins = n_bins
        self.n_cats = n_cats
        self.n_nums = n_nums
        self.cards = cards
        self.n_disc = self.n_bins + self.n_cats
        self.num_categorical_list = [2]*self.n_bins + self.cards

        if self.n_disc != 0:
            self.embeddings_list = nn.ModuleList()
            for num_categories in self.num_categorical_list:
                embedding = nn.Embedding(num_categories, emb_dim)
                self.embeddings_list.append(embedding)

        if self.n_nums != 0:
            self.mlp_nums = nn.Sequential(
                nn.Linear(n_nums, n_nums),
                nn.SiLU(),
                nn.Linear(n_nums, n_nums)
            )

        self.mlp_output = nn.Sequential(
            nn.Linear(emb_dim * self.n_disc + n_nums, emb_dim),
            nn.ReLU(),
            nn.Linear(emb_dim, emb_dim)
        )

    def forward(self, x):
        dev = x.device
        x_disc = x[:,:,0:self.n_disc].long().to(dev)
        x_nums = x[:,:,self.n_disc:self.n_disc+self.n_nums].to(dev)
        x_emb = torch.Tensor().to(dev)

        if self.n_disc != 0:
            variable_embeddings = [embedding(x_disc[:,:,i]) for i, embedding in enumerate(self.embeddings_list)]
            x_disc_emb = torch.cat(variable_embeddings, dim=2)
            x_emb = x_disc_emb

        if self.n_nums != 0:
            x_nums_emb = self.mlp_nums(x_nums)
            x_emb = torch.cat([x_emb, x_nums_emb], dim=2)

        final_emb = self.mlp_output(x_emb)
        return final_emb


class DeapStack(nn.Module):
    def __init__(self, channels, batch_size, seq_len, n_bins, n_cats, n_nums, cards, input_size,
                 hidden_size, num_layers, cat_emb_dim, time_dim, lat_dim, column_order):
        super().__init__()
        self.Emb = Embedding_data_auto(input_size, cat_emb_dim, n_bins, n_cats, n_nums, cards)
        self.time_encode = nn.Sequential(
            nn.Linear(time_dim, input_size),
            nn.ReLU(),
            nn.Linear(input_size, input_size)
        )

        self.encoder_mu = nn.GRU(input_size, hidden_size, num_layers, batch_first=True)
        self.encoder_logvar = nn.GRU(input_size, hidden_size, num_layers, batch_first=True)

        self.fc_mu = nn.Linear(hidden_size, lat_dim)
        self.fc_logvar = nn.Linear(hidden_size, lat_dim)

        self.decoder_mlp = nn.Sequential(
            nn.Linear(lat_dim, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size)
        )

        self.config = {
            'channels': channels,
            'batch_size': batch_size,
            'seq_len': seq_len,
            'n_bins': n_bins,
            'n_cats': n_cats,
            'n_nums': n_nums,
            'cards': cards,
            'input_size': input_size,
            'hidden_size': hidden_size,
            'num_layers': num_layers,
            'cat_emb_dim': cat_emb_dim,
            'time_dim': time_dim,
            'lat_dim': lat_dim,
            'column_order': column_order
        }

        self.lat_dim = lat_dim
        self.channels = channels
        self.n_bins = n_bins
        self.n_cats = n_cats
        self.n_nums = n_nums
        self.column_order = column_order    
        self.cards = cards
        self.disc = self.n_bins + self.n_cats
        self.sigmoid = torch.nn.Sigmoid()

        self.bins_linear = nn.Linear(hidden_size, n_bins) if n_bins else None
        self.cats_linears = nn.ModuleList([nn.Linear(hidden_size, card) for card in cards]) if n_cats else None 
        self.nums_linear = nn.Linear(hidden_size, n_nums) if n_nums else None

    def save_model(self, save_path):
        torch.save({
            'model_state_dict': self.state_dict(),
            'config': self.config
        }, f"{save_path}.pt")

    @classmethod
    def load_model(cls, load_path, device='cuda' if torch.cuda.is_available() else 'cpu'):
        checkpoint = torch.load(load_path, map_location=device, weights_only=False)
        model = cls(**checkpoint['config'])
        model.load_state_dict(checkpoint['model_state_dict'])
        model = model.to(device)
        return model

    def reparametrize(self, mu, logvar):
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mu + eps * std

    def encoder(self, x):
        x = self.Emb(x)
        mu_z, _ = self.encoder_mu(x)
        logvar_z, _ = self.encoder_logvar(x)
        mu_z = self.fc_mu(mu_z)
        logvar_z = self.fc_logvar(logvar_z)
        emb = self.reparametrize(mu_z, logvar_z)
        return emb, mu_z, logvar_z

    def decoder(self, latent_feature):
        decoded_outputs = dict()
        latent_feature = self.decoder_mlp(latent_feature)
        B, L, K = latent_feature.shape

        if self.bins_linear:
            decoded_outputs['bins'] = self.bins_linear(latent_feature)

        if self.cats_linears:
            decoded_outputs['cats'] = [linear(latent_feature) for linear in self.cats_linears]

        if self.nums_linear:
            decoded_outputs['nums'] = self.sigmoid(self.nums_linear(latent_feature))

        return decoded_outputs

    def forward(self, x):
        emb, mu_z, logvar_z = self.encoder(x)
        outputs = self.decoder(emb)
        return outputs, emb, mu_z, logvar_z


class PositionalEncoding(nn.Module):
    def __init__(self, dim: int, max_value: float):
        super().__init__()
        self.max_value = max_value
        linear_dim = dim // 2
        periodic_dim = dim - linear_dim
        self.scale = torch.exp(-2 * torch.arange(0, periodic_dim).float() * math.log(self.max_value) / periodic_dim)
        self.shift = torch.zeros(periodic_dim)
        self.shift[::2] = 0.5 * math.pi
        self.linear_proj = nn.Linear(1, linear_dim)

    def forward(self, t):
        periodic = torch.sin(t * self.scale.to(t) + self.shift.to(t))
        linear = self.linear_proj(t / self.max_value)
        return torch.cat([linear, periodic], -1)


class FeedForward(nn.Module):
    def __init__(self, in_dim: int, hidden_dims: List[int], out_dim: int, activation: Callable=nn.ReLU(), final_activation: Callable=None):
        super().__init__()
        hidden_dims = hidden_dims[:]
        hidden_dims.append(out_dim)
        layers = [nn.Linear(in_dim, hidden_dims[0])]
        for i in range(len(hidden_dims) - 1):
            layers.append(activation)
            layers.append(nn.Linear(hidden_dims[i], hidden_dims[i+1]))
        if final_activation is not None:
            layers.append(final_activation)
        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x)


class BiRNN_score(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, diffusion_steps,
                 cond_dim, time_dim, emb_dim, n_bins, n_cats, n_nums, cards, column_order):
        super(BiRNN_score, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.config = {
            'input_size': input_size,
            'hidden_size': hidden_size,
            'num_layers': num_layers,
            'diffusion_steps': diffusion_steps,
            'cond_dim': cond_dim,
            'time_dim': time_dim,
            'emb_dim': emb_dim,
            'n_bins': n_bins,
            'n_cats': n_cats,
            'n_nums': n_nums,
            'cards': cards,
            'column_order': column_order
        }

        self.input_proj = FeedForward(input_size, [], hidden_size)
        self.t_enc = PositionalEncoding(hidden_size, max_value=1)
        self.i_enc = PositionalEncoding(hidden_size, max_value=diffusion_steps) 
        self.proj = FeedForward(4 * hidden_size, [], hidden_size, final_activation=nn.ReLU())

        self.lstm = nn.LSTM(hidden_size, hidden_size, num_layers, batch_first=True, bidirectional=True)
        self.layer_norm = nn.LayerNorm(2 * hidden_size)
        self.fc = nn.Linear(2 * hidden_size, input_size)

        self.Emb = Embedding_data_diff(input_size, emb_dim, n_bins, n_cats, n_nums, cards)
        self.cond_lstm = nn.LSTM(emb_dim, hidden_size, num_layers, batch_first=True, bidirectional=True)
        self.cond_output = nn.Linear(2*hidden_size, hidden_size)

        self.time_encode = nn.Sequential(
            nn.Linear(time_dim, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size)
        )

    def save_model(self, save_path):
        torch.save({
            'model_state_dict': self.state_dict(),
            'config': self.config
        }, f"{save_path}.pt")

    @classmethod
    def load_model(cls, load_path, device='cuda' if torch.cuda.is_available() else 'cpu'):
        checkpoint = torch.load(load_path, map_location=device, weights_only=False)
        model = cls(**checkpoint['config'])
        model.load_state_dict(checkpoint['model_state_dict'])
        model = model.to(device)
        return model

    def forward(self, x, t, i, cond=None, time_info=None):
        shape = x.shape
        x = x.view(-1, *shape[-2:])
        t = t.view(-1, shape[-2], 1)
        i = i.view(-1, shape[-2], 1)

        x = self.input_proj(x)
        t = self.t_enc(t)
        i = self.i_enc(i)
        time_info = self.time_encode(time_info)

        if cond is not None:            
            cond_out, _ = self.cond_lstm(self.Emb(cond))
            x = self.proj(torch.cat([x + self.cond_output(cond_out), t, i, time_info], -1))    
        else:
            x = self.proj(torch.cat([x, t, i, time_info], -1))

        out, _ = self.lstm(x)
        output = self.layer_norm(out)
        final_out = self.fc(output)
        return final_out


@torch.no_grad()
def sample(t, B, T, F, model, cond, time_info):
    dev = next(model.parameters()).device
    x = torch.randn(B, T, F).to(dev)
    time_info = time_info.to(dev)
    column_order = model.config['column_order']
    cond = cond[:,:,column_order].to(dev)

    for diff_step in reversed(range(0, diffusion_steps)):
        alpha = alphas[diff_step].to(dev)
        beta = betas[diff_step].to(dev)
        z = torch.randn(B, T, F).to(dev)
        i = torch.Tensor([diff_step]).expand_as(x[...,:1]).to(dev)
        cond_noise = model(x, t, i, cond, time_info)
        x = (1/(1 - beta).sqrt()) * (x - beta * cond_noise / (1 - alpha).sqrt()) + beta.sqrt() * z

    return x
