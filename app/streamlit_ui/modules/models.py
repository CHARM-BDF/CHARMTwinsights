"""
Models page for CHARMTwinsights
"""

import streamlit as st
from api_client import get_available_models, predict_with_model
from utils import create_model_input_form


def show_model_marketplace():
    """Model marketplace and testing interface"""
    st.header("Models")
    st.markdown("Explore and test available models")
    
    # Fetch available models
    models = get_available_models()
    
    if models:
        st.success(f"📊 Found {len(models)} available models")
        
        # Model cards
        for model in models:
            with st.container():
                st.markdown(f"""
                <div class="model-card">
                    <h3>{model.get('title', 'Unknown Model')}</h3>
                    <p><strong>Image:</strong> {model.get('image', 'N/A')}</p>
                    <p><strong>Description:</strong> {model.get('short_description', 'No description available')}</p>
                    <p><strong>Authors:</strong> {model.get('authors', 'Unknown')}</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Model testing section
                with st.expander(f"Test {model.get('title', 'Model')}"):
                    if model.get("examples"):
                        st.subheader("Example Inputs")
                        
                        # Show first example
                        example = model["examples"][0]
                        
                        # Create input form based on example
                        st.markdown("**Modify input parameters:**")
                        
                        # Dynamic input creation
                        test_input = create_model_input_form(example, model['image'])
                        
                        if st.button(f"🚀 Run Prediction", key=f"predict_{model['image']}"):
                            with st.spinner("Running prediction..."):
                                result = predict_with_model(model["image"], [test_input])
                                
                                if result["success"]:
                                    data = result["data"]
                                    
                                    st.markdown("""
                                    <div class="prediction-result">
                                        <h4>Prediction Results</h4>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Display predictions
                                    if "predictions" in data:
                                        st.json(data["predictions"])
                                    
                                    # Display logs if available
                                    if data.get("stderr"):
                                        with st.expander("📝 Model Logs"):
                                            st.code(data["stderr"])
                                else:
                                    st.error(f"Prediction failed: {result['error']}")
                    else:
                        st.info("No examples available for this model")
    else:
        st.warning("No models are currently registered")
