"""
TimeAutoDiff synthetic timeseries generation module.

This module provides endpoints for generating synthetic ICU timeseries data
using the TimeAutoDiff diffusion model trained on MIMIC-III data.
"""

import os
import json
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

import torch
import numpy as np

logger = logging.getLogger(__name__)

# Model directory path
MODEL_DIR = Path(__file__).parent.parent / "timeseries-generative-model" / "timeautodiff"
METADATA_PATH = MODEL_DIR / "metadata.json"

# Global model cache
_models_cache: Dict[str, Any] = {}
_metadata_cache: Optional[Dict[str, Any]] = None


def get_metadata() -> Dict[str, Any]:
    """Load and cache model metadata."""
    global _metadata_cache
    if _metadata_cache is None:
        with open(METADATA_PATH, 'r') as f:
            _metadata_cache = json.load(f)
    return _metadata_cache


def get_device() -> str:
    """Get the best available device."""
    if torch.cuda.is_available():
        return 'cuda'
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        return 'mps'
    return 'cpu'


def load_models(device: Optional[str] = None) -> Dict[str, Any]:
    """
    Load TimeAutoDiff models (autoencoder, diffusion, latent features).
    
    Args:
        device: Target device ('cpu', 'cuda', 'mps'). Auto-detected if None.
    
    Returns:
        Dictionary containing loaded models and metadata.
    """
    global _models_cache
    
    if device is None:
        device = get_device()
    
    cache_key = f"models_{device}"
    if cache_key in _models_cache:
        return _models_cache[cache_key]
    
    logger.info(f"Loading TimeAutoDiff models on device: {device}")
    
    # Use vendored timeautodiff package
    from timeautodiff import timeautodiff_v4_efficient_simple as timeautodiff_module
    
    ae_path = MODEL_DIR / "autoencoder.pt"
    diff_path = MODEL_DIR / "diffusion.pt"
    latent_path = MODEL_DIR / "latent_features.pt"
    
    # Load models
    ae = timeautodiff_module.DeapStack.load_model(str(ae_path))
    diff = timeautodiff_module.BiRNN_score.load_model(str(diff_path))
    latent_features = torch.load(str(latent_path), map_location=device)
    
    # Debug: Log the column_order from model configs
    logger.info(f"Autoencoder config: {ae.config}")
    logger.info(f"Autoencoder column_order: {ae.column_order}")
    logger.info(f"Diffusion config: {diff.config}")
    
    # Move to device
    ae = ae.to(device)
    diff = diff.to(device)
    
    ae.eval()
    diff.eval()
    
    metadata = get_metadata()
    
    result = {
        'autoencoder': ae,
        'diffusion': diff,
        'latent_features': latent_features,
        'metadata': metadata,
        'device': device,
        'seq_len': metadata.get('seq_len', 25),
        'n_features': metadata.get('number of features', 10),
        'feature_names': metadata.get('important_features_names', []),
    }
    
    _models_cache[cache_key] = result
    logger.info("TimeAutoDiff models loaded successfully")
    
    return result


def create_conditioning_tensor(
    batch_size: int,
    seq_len: int,
    ethnicity: Optional[List[int]] = None,
    gender: Optional[List[int]] = None,
    age_group: Optional[List[int]] = None,
    mortality_label: Optional[List[int]] = None,
    device: str = 'cpu'
) -> torch.Tensor:
    """
    Create conditioning tensor for generation.
    
    Args:
        batch_size: Number of samples to generate
        seq_len: Sequence length (timesteps)
        ethnicity: List of ethnicity codes (0-4) per sample, or None for random
        gender: List of gender codes (0-1) per sample, or None for random
        age_group: List of age group codes (0-3) per sample, or None for random
        mortality_label: List of mortality labels (0-1) per sample, or None for random
        device: Target device
    
    Returns:
        Conditioning tensor of shape (batch_size, seq_len, 4)
    """
    # Generate random values if not provided
    # Note: ethnicity is 0-3, gender is 0-1, age_group is 0-3, mortality_label is 0-1
    if ethnicity is None:
        ethnicity = torch.randint(0, 4, (batch_size,)).tolist()
    if gender is None:
        gender = torch.randint(0, 2, (batch_size,)).tolist()
    if age_group is None:
        age_group = torch.randint(0, 4, (batch_size,)).tolist()
    if mortality_label is None:
        mortality_label = torch.randint(0, 2, (batch_size,)).tolist()
    
    # Build conditioning tensor
    # Model's column_order=[0,3,1,2] reorders input as: out[i] = in[column_order[i]]
    # Embeddings expect [2, 2, 4, 4] = [binary, binary, cat4, cat4]
    # After reorder we need: [gender, mortality, ethnicity, age_group]
    # Working backwards: input must be [gender, ethnicity, age_group, mortality]
    # Because: out[0]=in[0]=gender, out[1]=in[3]=mortality, out[2]=in[1]=ethnicity, out[3]=in[2]=age_group
    cond = torch.zeros(batch_size, seq_len, 4, dtype=torch.float32)
    
    for i in range(batch_size):
        cond[i, :, 0] = gender[i] if i < len(gender) else 0           # -> position 0 after reorder (binary)
        cond[i, :, 1] = ethnicity[i] if i < len(ethnicity) else 0     # -> position 2 after reorder (cat4)
        cond[i, :, 2] = age_group[i] if i < len(age_group) else 0     # -> position 3 after reorder (cat4)
        cond[i, :, 3] = mortality_label[i] if i < len(mortality_label) else 0  # -> position 1 after reorder (binary)
    
    return cond.to(device)


def create_time_tensor(batch_size: int, seq_len: int, device: str = 'cpu') -> torch.Tensor:
    """
    Create time encoding tensor with cyclical encoding.
    
    Args:
        batch_size: Number of samples
        seq_len: Sequence length
        device: Target device
    
    Returns:
        Time tensor of shape (batch_size, seq_len, 4) with (day_sin, day_cos, hour_sin, hour_cos)
    """
    from timeautodiff import processing_simple
    # Use the same cyclical encoding as the original training
    time_info = processing_simple.cyclical_encode_hourly(seq_len, batch_size)
    return torch.tensor(time_info, dtype=torch.float32).to(device)


def generate_synthetic_timeseries(
    n_samples: int = 10,
    ethnicity: Optional[List[int]] = None,
    gender: Optional[List[int]] = None,
    age_group: Optional[List[int]] = None,
    mortality_label: Optional[List[int]] = None,
    device: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate synthetic ICU timeseries data.
    
    Args:
        n_samples: Number of synthetic patients to generate
        ethnicity: List of ethnicity codes (0-4), one per sample
        gender: List of gender codes (0=female, 1=male), one per sample
        age_group: List of age group codes (0-3), one per sample
        mortality_label: List of mortality labels (0=survived, 1=died), one per sample
        device: Target device (auto-detected if None)
    
    Returns:
        Dictionary containing:
        - synthetic_data: Generated timeseries (n_samples, seq_len, n_features)
        - conditioning: The conditioning values used
        - feature_names: Names of the features
        - metadata: Model metadata
    """
    if device is None:
        device = get_device()
    
    models = load_models(device)
    seq_len = models['seq_len']
    
    # Create conditioning
    cond = create_conditioning_tensor(
        batch_size=n_samples,
        seq_len=seq_len,
        ethnicity=ethnicity,
        gender=gender,
        age_group=age_group,
        mortality_label=mortality_label,
        device=device
    )
    
    time_info = create_time_tensor(n_samples, seq_len, device)
    
    logger.info(f"Generating {n_samples} synthetic timeseries samples...")
    
    try:
        # Use vendored timeautodiff package
        from timeautodiff import helper_simple as tdf_helper
        
        # Generate synthetic data using the helper
        with torch.no_grad():
            synthetic_data = tdf_helper.generate_synthetic_data_simple(
                models={
                    'ae': models['autoencoder'],
                    'diff': models['diffusion'],
                    'latent_features': models['latent_features']
                },
                cond=cond,
                time_info=time_info
            )
        
        # Convert to numpy for JSON serialization
        if isinstance(synthetic_data, torch.Tensor):
            synthetic_data = synthetic_data.cpu().numpy()
        
        # Map numeric values to human-readable labels
        gender_map = {0: 'female', 1: 'male'}
        ethnicity_map = {0: 'white', 1: 'black', 2: 'asian', 3: 'other'}
        age_group_map = {0: '0-30', 1: '30-50', 2: '50-70', 3: '70-100'}
        mortality_map = {0: 'survived', 1: 'died'}
        
        # Extract conditioning values
        gender_vals = [int(v) for v in cond[:, 0, 0].cpu().tolist()]
        ethnicity_vals = [int(v) for v in cond[:, 0, 1].cpu().tolist()]
        age_group_vals = [int(v) for v in cond[:, 0, 2].cpu().tolist()]
        mortality_vals = [int(v) for v in cond[:, 0, 3].cpu().tolist()]
        
        result = {
            'synthetic_data': synthetic_data.tolist(),
            'shape': list(synthetic_data.shape),
            'n_samples': n_samples,
            'seq_len': seq_len,
            'n_features': models['n_features'],
            'feature_names': models['feature_names'],
            'conditioning': {
                'gender': [{'value': v, 'label': gender_map[v]} for v in gender_vals],
                'ethnicity': [{'value': v, 'label': ethnicity_map[v]} for v in ethnicity_vals],
                'age_group': [{'value': v, 'label': age_group_map[v]} for v in age_group_vals],
                'mortality_label': [{'value': v, 'label': mortality_map[v]} for v in mortality_vals],
            },
        }
        
        logger.info(f"Generated synthetic data with shape: {synthetic_data.shape}")
        return result
        
    except Exception as e:
        logger.error(f"Error generating synthetic data: {e}", exc_info=True)
        raise


def get_model_info() -> Dict[str, Any]:
    """
    Get information about the TimeAutoDiff model.
    
    Returns:
        Dictionary with model metadata and status.
    """
    metadata = get_metadata()
    
    return {
        'model_name': 'TimeAutoDiff',
        'model_version': metadata.get('model_version', 'unknown'),
        'task': metadata.get('task_name', 'mortality24'),
        'data_source': metadata.get('data_name', 'mimic'),
        'seq_len': metadata.get('seq_len', 25),
        'n_features': metadata.get('number of features', 10),
        'feature_names': metadata.get('important_features_names', []),
        'feature_definitions': {
            'fio2': {
                'full_name': 'Fraction of Inspired Oxygen',
                'unit': '%',
                'description': 'Oxygen concentration delivered to patient (0-100%)',
                'mimic_itemid': [223835, 3420],
            },
            'map': {
                'full_name': 'Mean Arterial Pressure',
                'unit': 'mmHg',
                'description': 'Average arterial pressure during cardiac cycle',
                'mimic_itemid': [220052, 220181, 225312],
            },
            'dbp': {
                'full_name': 'Diastolic Blood Pressure',
                'unit': 'mmHg',
                'description': 'Arterial pressure during heart relaxation',
                'mimic_itemid': [220051, 220180, 225310],
            },
            'o2sat': {
                'full_name': 'Oxygen Saturation (SpO2)',
                'unit': '%',
                'description': 'Peripheral oxygen saturation (normal: 95-100%)',
                'mimic_itemid': [220277, 646],
            },
            'hr': {
                'full_name': 'Heart Rate',
                'unit': 'bpm',
                'description': 'Beats per minute (normal: 60-100)',
                'mimic_itemid': [220045, 211],
            },
            'temp': {
                'full_name': 'Temperature',
                'unit': '°C',
                'description': 'Body temperature (normal: 36.5-37.5°C)',
                'mimic_itemid': [223761, 223762, 676],
            },
            'resp': {
                'full_name': 'Respiratory Rate',
                'unit': 'breaths/min',
                'description': 'Breathing rate (normal: 12-20)',
                'mimic_itemid': [220210, 618],
            },
            'sbp': {
                'full_name': 'Systolic Blood Pressure',
                'unit': 'mmHg',
                'description': 'Arterial pressure during heart contraction',
                'mimic_itemid': [220050, 220179, 225309],
            },
            'ph': {
                'full_name': 'Blood pH',
                'unit': 'pH units',
                'description': 'Arterial blood acidity/alkalinity (normal: 7.35-7.45)',
                'mimic_itemid': [220274, 780],
            },
            'lymph': {
                'full_name': 'Lymphocyte Count',
                'unit': '% or K/uL',
                'description': 'White blood cell differential component',
                'mimic_itemid': [51244, 51245],
            },
        },
        'conditioning_features': ['ethnicity', 'gender', 'age_group', 'mortality_label'],
        'conditioning_ranges': {
            'ethnicity': '0-3 (0=white, 1=black, 2=asian, 3=other)',
            'gender': '0-1 (0=female, 1=male)',
            'age_group': '0-3 (0=0-30, 1=30-50, 2=50-70, 3=70-100)',
            'mortality_label': '0-1 (0=survived, 1=died)',
        },
        'training_info': {
            'timestamp': metadata.get('genmodel_timestamp', 'unknown'),
            'patient_count': metadata.get('patient_length', 0),
            'train_fraction': metadata.get('train_fraction', 0),
            'val_fraction': metadata.get('val_fraction', 0),
            'holdout_fraction': metadata.get('holdout_fraction', 0),
            'diffusion_steps': metadata.get('diffusion_steps', 100),
            'device': metadata.get('device', 'unknown'),
            'seed': metadata.get('seed', 0),
        },
        'model_architecture': {
            'autoencoder': {
                'hidden_size': metadata.get('auto_hidden_size', 0),
                'num_layers': metadata.get('auto_num_layers', 0),
                'batch_size': metadata.get('auto_batch_size', 0),
                'channels': metadata.get('auto_channels', 0),
                'latent_dim': metadata.get('auto_lat_dim', 0),
                'time_dim': metadata.get('auto_time_dim', 0),
                'emb_dim': metadata.get('auto_emb_dim', 0),
                'training_epochs': metadata.get('VAE_training', 0),
            },
            'diffusion': {
                'hidden_dim': metadata.get('diff_hidden_dim', 0),
                'num_layers': metadata.get('diff_num_layers', 0),
                'diffusion_steps': metadata.get('diffusion_steps', 100),
                'training_epochs': metadata.get('diff_training', 0),
            },
        },
        'preprocessing': {
            'numerical_processing': metadata.get('numerical_processing', 'unknown'),
            'imputation_strategy': metadata.get('imputation strategy', 'unknown'),
            'standardize': metadata.get('standardize', False),
            'processed_data_timestamp': metadata.get('processed_data_timestamp', 'unknown'),
        },
    }


def generate_timeseries_visualization(
    ethnicity: Optional[int] = None,
    gender: Optional[int] = None,
    age_group: Optional[int] = None,
    mortality_label: Optional[int] = None,
) -> str:
    """
    Generate synthetic ICU timeseries and return an interactive Plotly HTML visualization.
    
    Args:
        ethnicity: Ethnicity code (0-3), or None for random
        gender: Gender code (0-1), or None for random
        age_group: Age group code (0-3), or None for random
        mortality_label: Mortality label (0-1), or None for random
    
    Returns:
        HTML string containing interactive Plotly visualization
    """
    import plotly.graph_objects as go
    
    # Generate the data
    result = generate_synthetic_timeseries(
        n_samples=1,
        ethnicity=[ethnicity] if ethnicity is not None else None,
        gender=[gender] if gender is not None else None,
        age_group=[age_group] if age_group is not None else None,
        mortality_label=[mortality_label] if mortality_label is not None else None,
    )
    
    # Extract data
    data = np.array(result['synthetic_data'])[0]  # Shape: (25, 10)
    feature_names = result['feature_names']
    seq_len = result['seq_len']
    conditioning = result['conditioning']
    
    # Feature metadata for display
    feature_info = {
        'fio2': {'full_name': 'Fraction of Inspired Oxygen', 'unit': '%'},
        'map': {'full_name': 'Mean Arterial Pressure', 'unit': 'mmHg'},
        'dbp': {'full_name': 'Diastolic Blood Pressure', 'unit': 'mmHg'},
        'o2sat': {'full_name': 'Oxygen Saturation (SpO2)', 'unit': '%'},
        'hr': {'full_name': 'Heart Rate', 'unit': 'bpm'},
        'temp': {'full_name': 'Temperature', 'unit': '°C'},
        'resp': {'full_name': 'Respiratory Rate', 'unit': 'breaths/min'},
        'sbp': {'full_name': 'Systolic Blood Pressure', 'unit': 'mmHg'},
        'ph': {'full_name': 'Blood pH', 'unit': 'pH'},
        'lymph': {'full_name': 'Lymphocyte Count', 'unit': '%'},
    }
    
    # Create single figure with all traces
    fig = go.Figure()
    
    # Time axis (hours)
    time_hours = list(range(seq_len))
    
    # Add a trace for each feature - all on same plot
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    for i, feature in enumerate(feature_names):
        values = data[:, i]
        info = feature_info.get(feature, {'full_name': feature, 'unit': ''})
        
        # Create hover text with detailed info
        hover_text = [
            f"<b>{info['full_name']}</b><br>"
            f"Hour: {t}<br>"
            f"Value: {v:.2f} {info['unit']}"
            for t, v in zip(time_hours, values)
        ]
        
        fig.add_trace(
            go.Scatter(
                x=time_hours,
                y=values,
                mode='lines+markers',
                name=f"{info['full_name']} ({info['unit']})",
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=6),
                hovertemplate='%{text}<extra></extra>',
                text=hover_text,
                visible=True,  # All visible by default, can toggle via legend
            )
        )
    
    # Build patient info string
    gender_label = conditioning['gender'][0]['label']
    ethnicity_label = conditioning['ethnicity'][0]['label']
    age_label = conditioning['age_group'][0]['label']
    mortality_label_str = conditioning['mortality_label'][0]['label']
    
    patient_info = f"Gender: {gender_label} | Ethnicity: {ethnicity_label} | Age: {age_label} | Outcome: {mortality_label_str}"
    
    # Update layout - single plot with interactive legend
    fig.update_layout(
        title=dict(
            text=f"<b>Synthetic ICU Vitals Timeseries</b><br><sup>{patient_info}</sup>",
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="Hour",
        yaxis_title="Value (normalized)",
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
            itemclick="toggle",
            itemdoubleclick="toggleothers",
        ),
        hovermode='x unified',
        template='plotly_white',
        margin=dict(l=60, r=40, t=100, b=150),
    )
    
    # Return as standalone HTML
    html = fig.to_html(
        full_html=True,
        include_plotlyjs='cdn',
        config={
            'displayModeBar': True,
            'displaylogo': False,
            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
        }
    )
    
    return html
