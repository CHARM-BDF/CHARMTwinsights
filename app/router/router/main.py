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

@app.get("/health")
async def health_check():
    """Router service health check"""
    try:
        # Test connections to all backend services
        backend_services = {
            "model_server": f"{settings.model_server_url}/health",
            "stat_server_py": f"{settings.stat_server_py_url}/health",
            "stat_server_r": "http://stat_server_r:8000/health",
            "synthea_server": f"{settings.synthea_server_url}/health",
            "hapi_fhir": "http://hapi:8080/fhir/$meta",
            "model_server_db": "mongodb://model_server_db:27017/"
        }
        
        dependencies = {}
        async with httpx.AsyncClient(timeout=5.0) as client:
            for service_name, url in backend_services.items():
                try:
                    if service_name == "model_server_db":
                        # For MongoDB, we'll just check if the service is reachable
                        response = await client.get("http://model_server_db:27017/")
                        # MongoDB returns connection info, any response means it's up
                        dependencies[service_name] = {
                            "connected": True,
                            "url": url,
                            "error": None
                        }
                    else:
                        response = await client.get(url)
                        dependencies[service_name] = {
                            "connected": response.status_code == 200,
                            "url": url,
                            "error": None if response.status_code == 200 else f"HTTP {response.status_code}"
                        }
                except Exception as e:
                    dependencies[service_name] = {
                        "connected": False,
                        "url": url,
                        "error": str(e)
                    }
        
        return {
            "status": "healthy",
            "service": "router",
            "dependencies": dependencies
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "router",
            "error": str(e),
            "dependencies": {}
        }


@app.get("/modeling/health")
async def modeling_health_proxy():
    """Proxy to model server health endpoint"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.model_server_url}/health")
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=503, detail="Model server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Model server unreachable: {str(e)}")

@app.get("/stats/health")
async def stats_health_proxy():
    """Proxy to stats server (Python) health endpoint"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.stat_server_py_url}/health")
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=503, detail="Stats server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Stats server unreachable: {str(e)}")

@app.get("/stats-r/health")
async def stats_r_health_proxy():
    """Proxy to stats server (R) health endpoint"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("http://stat_server_r:8000/health")
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=503, detail="Stats R server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Stats R server unreachable: {str(e)}")

@app.get("/synthetic/health")
async def synthetic_health_proxy():
    """Proxy to synthea server health endpoint"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{settings.synthea_server_url}/health")
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=503, detail="Synthea server unhealthy")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Synthea server unreachable: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
