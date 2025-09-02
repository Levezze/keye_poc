"""
Keye POC API - Main FastAPI Application
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config.settings import settings
from api.v1.routes import router as v1_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    print("Starting Keye POC API...")
    yield
    # Shutdown
    print("Shutting down Keye POC API...")


app = FastAPI(
    title="Keye POC API",
    description="Data analysis pipeline with concentration analysis and AI insights",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS
# Configure CORS from settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Mount API v1
app.include_router(v1_router, prefix="/api/v1", tags=["v1"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "keye-poc-api"}


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Keye POC API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health",
    }
