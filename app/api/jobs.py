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

import uuid
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, UploadFile

from app.core.config import Settings, get_settings
from app.models.job import DynamoDBJobItem, JobResponse, JobStatus, JobSubmitResponse
from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService

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
    dynamodb_service: DynamoDBService = Depends(get_dynamodb_service),
    s3_service: S3Service = Depends(get_s3_service),
    sqs_service: SQSService = Depends(get_sqs_service),
) -> JobSubmitResponse:
    """Submit a document for async processing.

    Why 202?
    --------
    Processing is asynchronous — the worker picks up the job from SQS and may
    take seconds to minutes to complete.  202 Accepted signals to the caller
    that the request has been received and will be acted upon, but the result
    is not yet available.  Clients should poll GET /jobs/{job_id} for status.

    Flow:
    1. Create a PENDING job record in DynamoDB (idempotent guard against double-submit).
    2. Upload the raw document to S3.
    3. Update s3_input_key in DynamoDB so the worker knows where to find the file.
    4. Send a message to SQS so the worker picks it up.
    5. Return 202 with the job_id for polling.
    """
    job_id = str(uuid.uuid4())
    effective_filename = filename or file.filename or "document"
    content_type = file.content_type or "application/octet-stream"
    now = datetime.now(tz=timezone.utc)

    # 1. Create the job record in DynamoDB with PENDING status.
    job_item = DynamoDBJobItem(
        job_id=job_id,
        status=JobStatus.PENDING,
        filename=effective_filename,
        created_at=now,
        updated_at=now,
        s3_input_key="",  # will be updated after S3 upload
    )
    dynamodb_service.create_job(job_item)

    # 2. Upload the document to S3.
    content = await file.read()
    s3_key = s3_service.upload_document(job_id, effective_filename, content, content_type)

    # 3. Persist the S3 key back to DynamoDB so the worker knows where to fetch.
    _set_s3_input_key(dynamodb_service, job_id, s3_key)

    # 4. Enqueue the job for the worker.
    sqs_service.send_job(job_id)

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

    When the job is COMPLETED and a result_url is recorded, a fresh presigned
    URL is generated on every request rather than stored permanently.  This keeps
    the DynamoDB record free of time-limited secrets and ensures the URL the
    client receives is always valid for the full expiry window.
    """
    job_item: DynamoDBJobItem = dynamodb_service.get_job(job_id)

    result_url: str | None = None
    if job_item.status == JobStatus.COMPLETED and job_item.result_url:
        # result_url stored in DynamoDB is the presigned URL set by the worker;
        # regenerate a fresh one so it never expires for the caller.
        result_key = f"{job_id}/output/result.json"
        result_url = s3_service.generate_presigned_url(result_key)
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
