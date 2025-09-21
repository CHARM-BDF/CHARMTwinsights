from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
import httpx

# Import routers
from .routers import synthea
from .routers import modeling
from .routers import stat_server_py
from .config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

app = FastAPI(
    title="CHARMTwinsight API Gateway",
    description="Frontend REST API for CHARMTwinsight microservices.",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS for all origins for development; restrict for prod
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Set to your frontend domains in production!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(synthea.router)
app.include_router(modeling.router)
app.include_router(stat_server_py.router)

@app.get("/healthz")
async def health_check():
    return {"status": "ok"}

@app.get("/modeling/health")
async def modeling_health_check():
    """Check if the model server is reachable"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.model_server_url}/health")
            if response.status_code == 200:
                return {"service": "model_server", "status": "healthy"}
            else:
                raise HTTPException(status_code=503, detail="Model server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Model server unreachable: {str(e)}")

@app.get("/stats/health")
async def stats_health_check():
    """Check if the stats server is reachable"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.stat_server_py_url}/docs")
            if response.status_code == 200:
                return {"service": "stat_server_py", "status": "healthy"}
            else:
                raise HTTPException(status_code=503, detail="Stats server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Stats server unreachable: {str(e)}")

@app.get("/synthetic/health")
async def synthetic_health_check():
    """Check if the synthea server is reachable"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.synthea_server_url}/docs")
            if response.status_code == 200:
                return {"service": "synthea_server", "status": "healthy"}
            else:
                raise HTTPException(status_code=503, detail="Synthea server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Synthea server unreachable: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
