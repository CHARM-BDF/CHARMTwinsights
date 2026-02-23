# Vendored timeautodiff package
# Source: https://github.com/mahmoudibrahim98/icu-autodiff
# File: timeautodiff/helper_simple.py

import torch
import numpy as np

from . import processing_simple as processing
from . import timeautodiff_v4_efficient_simple as timeautodiff


def restore_original_order_tensor_argsort(shuffled_tensor, col_order):
    """Alternative implementation using argsort for tensors"""
    if not isinstance(col_order, torch.Tensor):
        col_order = torch.tensor(col_order, device=shuffled_tensor.device)
    inverse_order = torch.argsort(col_order)
    original_tensor = shuffled_tensor[:, :, inverse_order]
    return original_tensor


def generate_synthetic_data_simple(models, cond, time_info, numerical_processing='normalize', unprocess=False):
    """
    Generate synthetic timeseries data using the trained models.
    
    Args:
        models: Dictionary containing 'ae' (autoencoder), 'diff' (diffusion model), 
                and 'latent_features'
        cond: Conditioning tensor [batch_size, seq_len, cond_dim]
        time_info: Time information tensor [batch_size, seq_len, time_dim]
        numerical_processing: Type of numerical processing used during training
        unprocess: Whether to unprocess (unnormalize) the data
    
    Returns:
        Synthetic data tensor [batch_size, seq_len, n_features]
    """
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    ae = models['ae'].to(device)
    diff = models['diff'].to(device)

    Batch_size, Seq_len, _ = cond.shape
    lat_dim = models['ae'].lat_dim

    # Create time grid and ensure correct shape
    t_grid = torch.linspace(0, 1, Seq_len).view(1, -1, 1).to(device).repeat(Batch_size, 1, 1)

    # Generate samples using diffusion model
    samples = timeautodiff.sample(t_grid, Batch_size, Seq_len, lat_dim, diff, cond, time_info)

    # Apply decoder to generated latent vector
    gen_output = ae.decoder(samples)

    datatype_info = {
        'n_bins': models['ae'].n_bins,
        'n_cats': models['ae'].n_cats,
        'n_nums': models['ae'].n_nums,
        'cards': models['ae'].cards
    }

    col_order = models['ae'].column_order
    synth_data = processing.convert_to_tensor(gen_output, Batch_size, Seq_len, datatype_info)
    synth_data = restore_original_order_tensor_argsort(synth_data, col_order)

    return synth_data


def generate_synthetic_data_in_batches(models, cond, time_info, batch_size=10000):
    """Generate synthetic data in batches to handle large datasets."""
    synth_data_batches = []
    num_samples = cond.size(0)

    for start in range(0, num_samples, batch_size):
        end = min(start + batch_size, num_samples)
        cond_batch = cond[start:end]
        time_info_batch = time_info[start:end]
        _synth_data = generate_synthetic_data_simple(models, cond_batch, time_info_batch)
        synth_data_batches.append(_synth_data)

    synth_data = torch.cat(synth_data_batches, dim=0)
    return synth_data
