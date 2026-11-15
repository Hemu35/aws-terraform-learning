"""
FastAPI Application Entry Point
================================
Document Processing API — local-first, backed by LocalStack (S3, SQS, DynamoDB).

Design decisions:
- Lifespan context manager replaces deprecated @app.on_event hooks (FastAPI ≥ 0.93).
- Global exception handlers translate domain errors to HTTP responses so routers
  stay clean.
- Health check is intentionally lightweight (no DB ping) to avoid cascading
  failures during container startup probes.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.config import get_settings
from app.core.exceptions import (
    AppError,
    InvalidFileTypeError,
    JobAlreadyExistsError,
    JobNotFoundError,
    PayloadTooLargeError,
    QueueError,
    StorageError,
)
from app.core.logging_config import get_logger, setup_logging

logger = get_logger(__name__)

APP_VERSION = "0.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown lifecycle."""
    settings = get_settings()
    setup_logging(settings.log_level)
    logger.info(
        "Document Processing API starting up",
        extra={"version": APP_VERSION, "environment": settings.environment},
    )
    yield
    logger.info("Document Processing API shutting down")


app = FastAPI(
    title="Document Processing API",
    description=(
        "Async document processing pipeline backed by S3 (storage), "
        "SQS (job queue) and DynamoDB (job state). Designed to run locally "
        "with LocalStack and in production on real AWS without code changes."
    ),
    version=APP_VERSION,
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(JobNotFoundError)
async def job_not_found_handler(request: Request, exc: JobNotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={"detail": exc.message, "error_code": "JOB_NOT_FOUND"},
    )


@app.exception_handler(JobAlreadyExistsError)
async def job_already_exists_handler(
    request: Request, exc: JobAlreadyExistsError
) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": exc.message, "error_code": "JOB_ALREADY_EXISTS"},
    )


@app.exception_handler(PayloadTooLargeError)
async def payload_too_large_handler(request: Request, exc: PayloadTooLargeError) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={"detail": exc.message, "error_code": "PAYLOAD_TOO_LARGE"},
    )


@app.exception_handler(InvalidFileTypeError)
async def invalid_file_type_handler(request: Request, exc: InvalidFileTypeError) -> JSONResponse:
    return JSONResponse(
        status_code=415,
        content={"detail": exc.message, "error_code": "UNSUPPORTED_MEDIA_TYPE"},
    )


@app.exception_handler(StorageError)
async def storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
    logger.error("Storage error", extra={"error_message": exc.message, "details": exc.details})
    return JSONResponse(
        status_code=503,
        content={"detail": "Storage service unavailable", "error_code": "STORAGE_ERROR"},
    )


@app.exception_handler(QueueError)
async def queue_error_handler(request: Request, exc: QueueError) -> JSONResponse:
    logger.error("Queue error", extra={"error_message": exc.message, "details": exc.details})
    return JSONResponse(
        status_code=503,
        content={"detail": "Queue service unavailable", "error_code": "QUEUE_ERROR"},
    )


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    logger.error("Application error", extra={"error_message": exc.message, "details": exc.details})
    return JSONResponse(
        status_code=500,
        content={"detail": exc.message, "error_code": "APP_ERROR"},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "Unhandled exception",
        extra={"path": str(request.url), "method": request.method},
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "error_code": "INTERNAL_ERROR"},
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

from app.api.jobs import router as jobs_router  # noqa: E402 — after app creation

app.include_router(jobs_router, prefix="/api/v1")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/health", tags=["health"])
async def health_check() -> dict:
    """Lightweight liveness probe used by Docker and load-balancer health checks."""
    return {"status": "ok", "version": APP_VERSION}
