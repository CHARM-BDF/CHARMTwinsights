"""
Configuration settings for CHARMTwinsights Streamlit app
"""

import os

# API Configuration
API_BASE = os.getenv("API_BASE", "http://localhost:8000")

# Service endpoints for health checks - all routed through the router service
SERVICES = {
    "Router": f"{API_BASE}/healthz",
    "Model Server": f"{API_BASE}/modeling/health",
    "Stats Server": f"{API_BASE}/stats/health", 
    "Synthea Server": f"{API_BASE}/synthetic/health"
}

# Page configuration
PAGE_CONFIG = {
    "page_title": "CHARMTwinsights",
    "page_icon": "🏥",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Custom CSS for styling
CUSTOM_CSS = """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f4e79;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 0.5rem;
        color: white;
        text-align: center;
    }
    .status-healthy {
        color: #28a745;
        font-weight: bold;
    }
    .status-unhealthy {
        color: #dc3545;
        font-weight: bold;
    }
    .model-card {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .prediction-result {
        background: #f8f9fa;
        border-left: 4px solid #007bff;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0 8px 8px 0;
    }
</style>
"""

# Default settings
DEFAULT_SETTINGS = {
    "timeout": 30,  # Increased from 10 to handle slower container startup
    "visualization_timeout": 30
}
