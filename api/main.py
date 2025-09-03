"""
Keye POC API - Main FastAPI Application
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
from config.settings import settings
from api.v1.routes import router as v1_router
from api.middleware import (
    RequestTrackingMiddleware,
    RateLimitMiddleware,
    http_exception_handler,
    validation_exception_handler,
    general_exception_handler,
)


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

# Add middleware in reverse order (last added = first executed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*", "X-Request-ID"],
    expose_headers=["X-Request-ID"],
)

# Add rate limiting middleware
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)

# Add request tracking middleware
app.add_middleware(RequestTrackingMiddleware)

# Add exception handlers
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(ValidationError, validation_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# Mount API v1
app.include_router(v1_router, prefix="/api/v1", tags=["v1"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "keye-poc-api"}


@app.head("/healthz")
@app.get("/healthz")
async def health_check_enhanced():
    """Enhanced health check endpoint that validates dependencies."""
    from pathlib import Path
    import os

    health_status = {"status": "healthy", "service": "keye-poc-api", "checks": {}}

    try:
        # Check storage directory exists and is writable
        storage_path = settings.datasets_path
        health_status["checks"]["storage_directory"] = {
            "status": "healthy" if storage_path.exists() else "unhealthy",
            "path": str(storage_path),
            "writable": (
                os.access(storage_path, os.W_OK) if storage_path.exists() else False
            ),
        }

        # Check if we can create directories
        test_dir = storage_path / "health_check"
        try:
            test_dir.mkdir(exist_ok=True)
            test_dir.rmdir()
            health_status["checks"]["directory_creation"] = {"status": "healthy"}
        except Exception as e:
            health_status["checks"]["directory_creation"] = {
                "status": "unhealthy",
                "error": str(e),
            }

        # Overall status
        all_healthy = all(
            check.get("status") == "healthy"
            for check in health_status["checks"].values()
        )

        if not all_healthy:
            health_status["status"] = "unhealthy"
            return JSONResponse(status_code=503, content=health_status)

    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["error"] = str(e)
        return JSONResponse(status_code=503, content=health_status)

    return health_status


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Keye POC API",
        "version": "0.1.0",
        "docs": "/docs",
        "health": "/health",
    }
