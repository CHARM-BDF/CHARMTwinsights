#!/bin/bash

# Build script for CHARMTwinsights
# This script builds both application images and model images in the correct order

set -e  # Exit on any error

echo "Building CHARMTwinsights..."

# Parse command line arguments
DOCKER_ARGS=""
FORCE_REBUILD=false
SKIP_MODELS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cache)
            DOCKER_ARGS="$DOCKER_ARGS $1"
            FORCE_REBUILD=true
            shift
            ;;
        --progress=*|--pull)
            DOCKER_ARGS="$DOCKER_ARGS $1"
            shift
            ;;
        --skip-models)
            SKIP_MODELS=true
            shift
            ;;
        --force-models)
            FORCE_REBUILD=true
            shift
            ;;
        *)
            echo "Unknown option $1"
            echo "Usage: $0 [--no-cache] [--progress=plain] [--pull] [--skip-models] [--force-models]"
            echo "  --no-cache     Force rebuild all images"
            echo "  --skip-models  Skip model image builds (faster for app development)"
            echo "  --force-models Force rebuild only model images"
            exit 1
            ;;
    esac
done

# Check if model images exist and are recent (unless forcing rebuild)
if [[ "$SKIP_MODELS" == "false" && "$FORCE_REBUILD" == "false" ]]; then
    echo "Checking if model images need rebuilding..."
    MODEL_IMAGES_EXIST=true
    
    # Check if key model images exist
    if ! docker image inspect coxcopdmodel:latest >/dev/null 2>&1; then
        echo "  coxcopdmodel:latest not found, will rebuild models"
        MODEL_IMAGES_EXIST=false
    elif ! docker image inspect irismodel:latest >/dev/null 2>&1; then
        echo "  irismodel:latest not found, will rebuild models"
        MODEL_IMAGES_EXIST=false
    else
        echo "  Model images exist, skipping rebuild (use --force-models to force)"
        SKIP_MODELS=true
    fi
fi

if [[ "$SKIP_MODELS" == "false" ]]; then
    echo "Step 1/2: Building model images..."
    cd model_server/models
    ./build_model_images.sh $DOCKER_ARGS
    cd ../..
else
    echo "Step 1/2: Skipping model images (already exist or --skip-models specified)"
fi

echo "Step 2/2: Building application images..."
docker compose build --parallel $DOCKER_ARGS

echo "Build complete! You can now run:"
echo "   docker compose up --detach"
echo ""
echo "Build tips:"
echo "   ./build_all.sh --skip-models     # Skip model builds (faster for app development)"
echo "   ./build_all.sh --force-models    # Force rebuild models only"
echo "   ./build_all.sh --no-cache        # Force rebuild everything"
