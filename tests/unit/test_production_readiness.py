"""
Production Readiness Integration and Failure Mode Tests.

Covers:
- Duplicate SQS delivery and worker idempotency
- Concurrent worker atomic job claiming
- Visibility timeout heartbeat extension
- DLQ redrive handling
- S3 upload failure handling (HTTP 503)
- DynamoDB conditional update guard failure
- Upload size limit enforcement (HTTP 413)
- Unsupported file type rejection (HTTP 415)
"""

from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
from httpx import ASGITransport, AsyncClient
from moto import mock_aws

from app.api.jobs import get_settings_dep
from app.core.config import Settings
from app.core.exceptions import StorageError
from app.main import app
from app.models.job import DynamoDBJobItem, JobStatus
from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService
from app.worker.processor import JobProcessor, VisibilityHeartbeat


def _create_test_settings() -> Settings:
    return Settings(
        aws_default_region="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_endpoint_url=None,
        dynamodb_table_name="jobs",
        s3_bucket_name="documents",
        sqs_queue_url="http://moto-sqs/queue/jobs-queue",
        log_level="WARNING",
        environment="test",
        max_upload_size_bytes=25 * 1024 * 1024,
    )


def _bootstrap_mock_aws(settings: Settings):
    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    s3.create_bucket(Bucket=settings.s3_bucket_name)

    dynamo = boto3.resource(
        "dynamodb",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    table = dynamo.create_table(
        TableName=settings.dynamodb_table_name,
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    sqs = boto3.client(
        "sqs",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    q = sqs.create_queue(QueueName="jobs-queue")
    settings.sqs_queue_url = q["QueueUrl"]
    return s3, table, sqs


# ===========================================================================
# 1. SQS Reliability & Worker Idempotency
# ===========================================================================


@mock_aws
def test_duplicate_sqs_delivery_worker_idempotency():
    """If a duplicate SQS message is delivered for an already completed job, the worker skips re-processing."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    s3 = S3Service(settings)
    processor = JobProcessor(settings)

    job_id = "duplicate-test-job-001"
    now = datetime.now(tz=timezone.utc)
    s3_key = s3.upload_document(job_id, "doc.txt", b"Test content", "text/plain")

    # Seed job in DynamoDB as PENDING
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="doc.txt",
            created_at=now,
            updated_at=now,
            s3_input_key=s3_key,
        )
    )

    # First delivery: worker processes job to completion
    first_run = processor.process_job(job_id)
    assert first_run is True

    job_after_first = dynamo.get_job(job_id)
    assert job_after_first.status == JobStatus.COMPLETED
    assert job_after_first.s3_output_key is not None

    # Duplicate delivery arrives: processor attempts to process the same job again
    second_run = processor.process_job(job_id)
    assert second_run is False  # Must be skipped cleanly

    # Verify status remains COMPLETED and was not corrupted
    job_after_duplicate = dynamo.get_job(job_id)
    assert job_after_duplicate.status == JobStatus.COMPLETED


@mock_aws
def test_concurrent_job_processing_atomic_claim():
    """Two concurrent workers trying to claim the same PENDING job — only one succeeds."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo_worker_1 = DynamoDBService(settings)
    dynamo_worker_2 = DynamoDBService(settings)

    job_id = "concurrent-claim-job-002"
    now = datetime.now(tz=timezone.utc)
    dynamo_worker_1.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="doc.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/doc.txt",
        )
    )

    # Worker 1 claims first
    claim_1 = dynamo_worker_1.claim_job(job_id)
    assert claim_1 is True

    # Worker 2 attempts to claim the same job simultaneously
    claim_2 = dynamo_worker_2.claim_job(job_id)
    assert claim_2 is False

    # Status must be PROCESSING, owned by Worker 1
    job = dynamo_worker_1.get_job(job_id)
    assert job.status == JobStatus.PROCESSING


def test_visibility_timeout_heartbeat_extension():
    """VisibilityHeartbeat periodically calls change_message_visibility to keep the message locked."""
    mock_sqs = MagicMock()
    receipt_handle = "test-receipt-handle-12345"

    with VisibilityHeartbeat(
        sqs=mock_sqs,
        receipt_handle=receipt_handle,
        interval_seconds=0.05,
        extension_seconds=120,
    ):
        time.sleep(0.12)  # Wait for at least 1-2 heartbeat ticks

    assert mock_sqs.change_message_visibility.called
    mock_sqs.change_message_visibility.assert_called_with(receipt_handle, 120)


@mock_aws
def test_dlq_redrive_policy_routing():
    """Messages failing maxReceiveCount attempts are routed to the DLQ."""
    settings = _create_test_settings()
    sqs_client = boto3.client("sqs", region_name=settings.aws_default_region)

    # Create DLQ
    dlq_res = sqs_client.create_queue(QueueName="test-dlq")
    dlq_url = dlq_res["QueueUrl"]
    dlq_attrs = sqs_client.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    # Create main queue with redrive policy (maxReceiveCount = 2)
    main_res = sqs_client.create_queue(
        QueueName="test-main-queue",
        Attributes={
            "VisibilityTimeout": "0",  # Immediate re-appearance for test
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 2}),
        },
    )
    main_url = main_res["QueueUrl"]

    # Send test poison message
    sqs_client.send_message(QueueUrl=main_url, MessageBody=json.dumps({"job_id": "poison-job"}))

    # Receive 1st time (receive count = 1)
    r1 = sqs_client.receive_message(QueueUrl=main_url)
    assert len(r1.get("Messages", [])) == 1

    # Receive 2nd time (receive count = 2)
    r2 = sqs_client.receive_message(QueueUrl=main_url)
    assert len(r2.get("Messages", [])) == 1

    # 3rd receive exceeds maxReceiveCount; message must be moved to DLQ
    r3 = sqs_client.receive_message(QueueUrl=main_url)
    assert "Messages" not in r3

    # Message is now in DLQ
    dlq_msgs = sqs_client.receive_message(QueueUrl=dlq_url)
    assert len(dlq_msgs.get("Messages", [])) == 1
    assert json.loads(dlq_msgs["Messages"][0]["Body"])["job_id"] == "poison-job"


# ===========================================================================
# 2. DynamoDB Consistency & Guard Tests
# ===========================================================================


@mock_aws
def test_dynamodb_conditional_update_guard_failure():
    """Updating a job to COMPLETED fails if it is not currently in PROCESSING status."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)
    dynamo = DynamoDBService(settings)
    job_id = "guard-failure-job"
    now = datetime.now(tz=timezone.utc)

    # Create job in PENDING status
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.pdf",
            created_at=now,
            updated_at=now,
            s3_input_key="in/test.pdf",
        )
    )

    # Attempting to jump directly from PENDING to COMPLETED with expected_status=PROCESSING must raise StorageError
    with pytest.raises(StorageError) as exc_info:
        dynamo.update_job_status(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            expected_status=JobStatus.PROCESSING,
        )

    assert "Conditional check failed" in str(exc_info.value)

    # Verify status remained PENDING
    current = dynamo.get_job(job_id)
    assert current.status == JobStatus.PENDING


# ===========================================================================
# 3. API Reliability, Validation & Failure Modes
# ===========================================================================


@pytest.mark.asyncio
async def test_api_s3_upload_failure_returns_503():
    """When an S3 upload fails, API handles StorageError and returns HTTP 503."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        try:
            with patch.object(S3Service, "upload_document", side_effect=StorageError("S3 connection timeout")):
                transport = ASGITransport(app=app)
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    files = {"file": ("report.pdf", io.BytesIO(b"Valid PDF Content"), "application/pdf")}
                    response = await client.post("/api/v1/jobs", files=files)

                assert response.status_code == 503
                data = response.json()
                assert data["error_code"] == "STORAGE_ERROR"
        finally:
            app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_payload_too_large_returns_413():
    """Uploading a document that exceeds max_upload_size_bytes returns HTTP 413."""
    settings = _create_test_settings()
    settings.max_upload_size_bytes = 100  # Set limit to 100 bytes for testing
    app.dependency_overrides[get_settings_dep] = lambda: settings

    oversized_content = b"X" * 150
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            files = {"file": ("large_doc.pdf", io.BytesIO(oversized_content), "application/pdf")}
            response = await client.post("/api/v1/jobs", files=files)

        assert response.status_code == 413
        data = response.json()
        assert data["error_code"] == "PAYLOAD_TOO_LARGE"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_unsupported_media_type_returns_415():
    """Uploading an unwhitelisted file extension and content type returns HTTP 415."""
    settings = _create_test_settings()
    app.dependency_overrides[get_settings_dep] = lambda: settings

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            files = {"file": ("malicious.exe", io.BytesIO(b"MZ..."), "application/x-msdownload")}
            response = await client.post("/api/v1/jobs", files=files)

        assert response.status_code == 415
        data = response.json()
        assert data["error_code"] == "UNSUPPORTED_MEDIA_TYPE"
    finally:
        app.dependency_overrides.clear()
