"""
Unit tests for the FastAPI HTTP endpoints.

Validates the full request/response cycle against mocked AWS services
(S3, SQS, DynamoDB via moto), testing status codes, payload structures,
and error mapping.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
import pytest
from httpx import ASGITransport, AsyncClient
from moto import mock_aws

from app.core.config import Settings
from app.main import app
from app.models.job import DynamoDBJobItem, JobStatus
from app.services.dynamodb_service import DynamoDBService
from app.api.jobs import get_settings_dep


@pytest.fixture
def override_settings(aws_settings: Settings):
    """Override FastAPI settings dependency with test settings."""
    app.dependency_overrides[get_settings_dep] = lambda: aws_settings
    yield aws_settings
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_health_endpoint():
    """Verify that the health check endpoint returns 200 and expected status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data


@pytest.mark.asyncio
async def test_submit_job_returns_202(
    override_settings, s3_bucket, dynamodb_table, sqs_queue
):
    """Submitting a document returns 202 Accepted with a job_id and PENDING status."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        files = {"file": ("report.pdf", io.BytesIO(b"Fake PDF content"), "application/pdf")}
        response = await client.post("/api/v1/jobs", files=files)

    assert response.status_code == 202
    data = response.json()
    assert "job_id" in data
    assert data["status"] == "PENDING"
    assert data["message"] == "Job submitted successfully"


@pytest.mark.asyncio
async def test_get_job_not_found_returns_404(
    override_settings, dynamodb_table
):
    """Polling a nonexistent job_id returns 404 with error detail."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/jobs/nonexistent-uuid")

    assert response.status_code == 404
    data = response.json()
    assert data["error_code"] == "JOB_NOT_FOUND"


@pytest.mark.asyncio
async def test_get_job_status_pending(
    override_settings, dynamodb_table, sample_job_item
):
    """Polling an existing PENDING job returns its metadata without result_url."""
    # Seed the job in DynamoDB
    db_service = DynamoDBService(override_settings)
    db_service.create_job(sample_job_item)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/v1/jobs/{sample_job_item.job_id}")

    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == sample_job_item.job_id
    assert data["status"] == "PENDING"
    assert data["filename"] == sample_job_item.filename
    assert data["result_url"] is None


@pytest.mark.asyncio
async def test_get_job_status_completed(
    override_settings, s3_bucket, dynamodb_table
):
    """Polling a COMPLETED job returns a newly generated presigned result URL."""
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    job_item = DynamoDBJobItem(
        job_id="completed-job-456",
        status=JobStatus.COMPLETED,
        filename="test.txt",
        created_at=now,
        updated_at=now,
        s3_input_key="completed-job-456/input/test.txt",
        result_url="https://s3.amazonaws.com/placeholder-presigned-url",
    )
    db_service = DynamoDBService(override_settings)
    db_service.create_job(job_item)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/jobs/completed-job-456")

    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == "completed-job-456"
    assert data["status"] == "COMPLETED"
    assert data["result_url"] is not None
    assert "completed-job-456/output/result.json" in data["result_url"]
