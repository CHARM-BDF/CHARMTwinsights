# CHARMTwinsights Clinical AI Research Studio

A professional Streamlit interface for researchers to explore synthetic data generation, AI model testing, and clinical prediction workflows.

## Features

### 🏠 Dashboard
- System health monitoring
- Quick access to key features
- Platform capability overview

### 🧬 Synthetic Data Laboratory
- Interactive cohort generation interface
- Customizable patient demographics
- Real-time generation with progress tracking
- FHIR and CSV export options

### 🤖 Model Marketplace
- Browse available AI models
- Interactive model testing with custom inputs
- Real-time prediction results
- Model documentation and examples

### 🔬 Research Workbench
- Multi-model comparison (coming soon)
- Batch processing capabilities
- Research pipeline management

### 📊 Analytics Hub
- Platform usage analytics
- Model performance insights
- System metrics visualization

## Usage

### Development (Local)
```bash
cd app/streamlit_ui
pip install -r requirements.txt
streamlit run app.py
```

### Production (Docker)
The Streamlit UI is included in the main docker-compose stack:

```bash
cd app
docker compose up streamlit_ui
```

Access at: http://localhost:8501

## Configuration

The app automatically detects the CHARMTwinsights API at `http://localhost:8000` when running locally, or uses the internal Docker network when running in containers.

## Professional Design

- Medical/clinical color scheme
- Professional data visualizations with Plotly
- Real-time system status monitoring
- Interactive model testing interface
- Responsive layout for research workflows

## API Integration

The app integrates with all CHARMTwinsights services:
- **Synthea Server**: Synthetic patient generation
- **Model Server**: AI model management and predictions
- **Stats Server**: Patient data analytics
- **Router**: Unified API gateway

Perfect for demonstrating the platform's capabilities to stakeholders and researchers!
