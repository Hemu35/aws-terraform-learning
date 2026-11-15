"""
Jobs API Router
===============
Exposes two REST endpoints for the document processing pipeline:

  POST /jobs         — Upload a document and enqueue a processing job.
  GET  /jobs/{id}    — Poll the status of an existing job.

Design decisions:
- 202 Accepted (not 201 Created) on POST because the document is not yet
  processed when the response is returned — only the job has been queued.
  Returning 201 would imply the resource is immediately usable, which it is not.
- Services are injected via FastAPI Depends so they can be swapped in tests
  without monkey-patching globals.
- Presigned URLs are regenerated on every GET when the job is COMPLETED so we
  never store long-lived signed tokens in DynamoDB (reduces blast radius if the
  DB is compromised and avoids stale URLs after expiry).
- s3_input_key is updated via DynamoDBService's internal resource directly after
  upload so the worker can find the input file. The public API (update_job_status)
  only covers status transitions; this keeps the router from reimplementing update
  logic while still setting the key.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, UploadFile

from app.core.config import Settings, get_settings
from app.core.exceptions import (
    DataIntegrityError,
    InvalidFileTypeError,
    JobAlreadyExistsError,
    JobNotFoundError,
    PayloadTooLargeError,
    QueueError,
)
from app.core.logging_config import get_logger
from app.models.job import DynamoDBJobItem, JobResponse, JobStatus, JobSubmitResponse
from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService

logger: logging.Logger = get_logger(__name__)

router = APIRouter(tags=["jobs"])

# ---------------------------------------------------------------------------
# Dependency factories
# ---------------------------------------------------------------------------


def get_settings_dep() -> Settings:
    return get_settings()


def get_dynamodb_service(
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> DynamoDBService:
    return DynamoDBService(settings)


def get_s3_service(
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> S3Service:
    return S3Service(settings)


def get_sqs_service(
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> SQSService:
    return SQSService(settings)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_s3_input_key(dynamodb_service: DynamoDBService, job_id: str, s3_key: str) -> None:
    """Write s3_input_key into DynamoDB after the upload succeeds.

    ``DynamoDBService.update_job_status`` handles status transitions; it does not
    accept arbitrary attribute updates.  We call the underlying DynamoDB table
    directly here — this is intentionally isolated to this single helper so the
    table interaction stays explicit and easy to find.
    """
    from boto3.dynamodb.conditions import Attr

    dynamodb_service._table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET s3_input_key = :key, updated_at = :ua",
        ExpressionAttributeValues={
            ":key": s3_key,
            ":ua": datetime.now(tz=timezone.utc).isoformat(),
        },
        ConditionExpression=Attr("job_id").exists(),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/jobs", status_code=202, response_model=JobSubmitResponse)
async def submit_job(
    file: Annotated[UploadFile, File(description="Document to process")],
    filename: Annotated[
        str | None,
        Form(description="Override the uploaded filename"),
    ] = None,
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", description="Optional client idempotency key for deduplication"),
    ] = None,
    x_idempotency_key: Annotated[
        str | None,
        Header(alias="X-Idempotency-Key", description="Alternative client idempotency key header"),
    ] = None,
    settings: Settings = Depends(get_settings_dep),
    dynamodb_service: DynamoDBService = Depends(get_dynamodb_service),
    s3_service: S3Service = Depends(get_s3_service),
    sqs_service: SQSService = Depends(get_sqs_service),
) -> JobSubmitResponse:
    """Submit a document for async processing.

    Why 202?
    --------
    Processing is asynchronous — the worker picks up the job from SQS and may
    take seconds to minutes to complete. 202 Accepted signals to the caller
    that the request has been received and will be acted upon, but the result
    is not yet available. Clients should poll GET /jobs/{job_id} for status.

    Production safeguards:
    - Content-type validation against allowed whitelist
    - Chunked streaming upload size enforcement to prevent memory exhaustion (DoS/OOM)
    - Client idempotency key support (Idempotency-Key / X-Idempotency-Key)
    - Streaming upload directly to S3 before DynamoDB record creation
    - Atomic DynamoDB job creation with s3_input_key already populated
    - SQS enqueue wrapped with DynamoDB rollback to FAILED on queue errors
    - Synchronous Boto3 SDK calls offloaded to worker threadpool via asyncio.to_thread
    - TTL expires_at metadata set on DynamoDB record
    """
    effective_filename = filename or file.filename or "document"
    content_type = file.content_type or "application/octet-stream"

    # 1. Content-Type validation
    allowed_types = settings.allowed_content_types
    is_allowed = (
        content_type in allowed_types
        or any(effective_filename.lower().endswith(ext) for ext in [".pdf", ".txt", ".csv", ".json", ".docx"])
    )
    if not is_allowed:
        logger.warning(
            "Rejected upload with unsupported content type",
            extra={"content_type": content_type, "doc_filename": effective_filename},
        )
        raise InvalidFileTypeError(content_type, allowed_types)

    # 2. Upload size validation (stream in chunks to temporary spool to avoid large memory allocations)
    max_bytes = settings.max_upload_size_bytes
    total_bytes = 0
    chunk_size = 1024 * 1024  # 1 MB

    with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as spool:
        while chunk := await file.read(chunk_size):
            total_bytes += len(chunk)
            if total_bytes > max_bytes:
                logger.warning(
                    "Rejected oversized document upload",
                    extra={"bytes_read": total_bytes, "max_bytes": max_bytes, "doc_filename": effective_filename},
                )
                raise PayloadTooLargeError(total_bytes, max_bytes)
            spool.write(chunk)

        spool.seek(0)

        # 3. Client Idempotency Check
        client_key = idempotency_key or x_idempotency_key
        if client_key:
            job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"urn:job:{client_key}"))
            try:
                existing_job = await asyncio.to_thread(dynamodb_service.get_job, job_id)
                logger.info(
                    "Idempotent job submission: returning existing job",
                    extra={"job_id": job_id, "idempotency_key": client_key, "status": existing_job.status},
                )
                return JobSubmitResponse(
                    job_id=existing_job.job_id,
                    status=existing_job.status,
                    message="Job already submitted (idempotent request)",
                )
            except JobNotFoundError:
                pass
        else:
            job_id = str(uuid.uuid4())

        # 4. Stream document to S3 without loading whole file into memory
        # We upload to S3 first so no incomplete/empty-key record is stored in DynamoDB
        s3_key = await asyncio.to_thread(
            s3_service.upload_fileobj, job_id, effective_filename, spool, content_type, total_bytes
        )

        # 5. Atomic Job creation with TTL and s3_input_key populated
        now = datetime.now(tz=timezone.utc)
        expires_at = int((now + timedelta(seconds=settings.job_ttl_seconds)).timestamp())

        job_item = DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename=effective_filename,
            created_at=now,
            updated_at=now,
            s3_input_key=s3_key,
            expires_at=expires_at,
        )

        logger.info(
            "Creating job record in DynamoDB", extra={"job_id": job_id, "s3_key": s3_key, "size_bytes": total_bytes}
        )
        try:
            await asyncio.to_thread(dynamodb_service.create_job, job_item)
        except JobAlreadyExistsError:
            if client_key:
                existing_job = await asyncio.to_thread(dynamodb_service.get_job, job_id)
                return JobSubmitResponse(
                    job_id=existing_job.job_id,
                    status=existing_job.status,
                    message="Job already submitted (idempotent request)",
                )
            raise

        # 6. Enqueue the job for the worker with safe rollback on SQS failure
        try:
            await asyncio.to_thread(sqs_service.send_job, job_id)
        except Exception as exc:
            logger.error(
                "SQS job enqueue failed — marking job FAILED in DynamoDB to prevent orphaned PENDING record",
                extra={"job_id": job_id, "error": str(exc)},
            )
            try:
                await asyncio.to_thread(
                    dynamodb_service.update_job_status,
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error_message="Failed to enqueue job to SQS",
                )
            except Exception as ddb_err:
                logger.error(
                    "Failed to update job status to FAILED after SQS failure",
                    extra={"job_id": job_id, "error": str(ddb_err)},
                )
            if isinstance(exc, QueueError):
                raise
            raise QueueError(f"Failed to enqueue job to SQS: {exc}") from exc

    logger.info("Job successfully queued", extra={"job_id": job_id, "s3_key": s3_key})

    return JobSubmitResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        message="Job submitted successfully",
    )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: str,
    dynamodb_service: DynamoDBService = Depends(get_dynamodb_service),
    s3_service: S3Service = Depends(get_s3_service),
) -> JobResponse:
    """Return the current status of a processing job.

    When the job is COMPLETED, a fresh presigned URL is generated dynamically
    from the authoritative stored S3 output key rather than guessing paths.
    """
    logger.info("Polling status for job", extra={"job_id": job_id})
    job_item: DynamoDBJobItem = await asyncio.to_thread(dynamodb_service.get_job, job_id)

    result_url: str | None = None
    if job_item.status == JobStatus.COMPLETED:
        if not job_item.s3_output_key:
            logger.error(
                "Data integrity error: COMPLETED job is missing authoritative s3_output_key",
                extra={"job_id": job_id},
            )
            raise DataIntegrityError(job_id)
        result_key = job_item.s3_output_key
        result_url = await asyncio.to_thread(s3_service.generate_presigned_url, result_key)
        # In local Docker Compose, translate internal container host to localhost for host clients
        if result_url and "http://localstack:4566" in result_url:
            result_url = result_url.replace("http://localstack:4566", "http://localhost:4566")

    return JobResponse(
        job_id=job_item.job_id,
        status=job_item.status,
        filename=job_item.filename,
        created_at=job_item.created_at,
        updated_at=job_item.updated_at,
        result_url=result_url,
        error_message=job_item.error_message,
    )
