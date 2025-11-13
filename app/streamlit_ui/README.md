# CHARMTwinsights Streamlit UI

## Structure

```
app/streamlit_ui/
├── app.py                  # Main application entry point
├── main.py                 # Alternative entry point (identical to app.py)
├── config.py               # Configuration settings and constants
├── api_client.py           # API interaction functions
├── utils.py                # Utility functions for data processing
├── components/             # Reusable UI components
│   ├── __init__.py
│   └── sidebar.py          # Sidebar navigation and status components
├── pages/                  # Page modules
│   ├── __init__.py
│   ├── dashboard.py        # System overview dashboard
│   ├── synthetic_data.py   # Synthetic data generation
│   ├── patient_browser.py  # Patient data browser and analytics
│   └── models.py           # Model marketplace and testing
├── requirements.txt        # Python dependencies
└── README.md       # This file
```
