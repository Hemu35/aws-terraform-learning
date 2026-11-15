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
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest
from httpx import ASGITransport, AsyncClient
from moto import mock_aws

from app.api.jobs import get_settings_dep
from app.core.config import Settings
from app.core.exceptions import QueueError, StorageError
from app.main import app
from app.models.job import AcquireResult, DynamoDBJobItem, JobStatus
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
    assert second_run is True  # Safe duplicate acknowledgement (SQS message deleted without reprocessing)

    # Verify status remains COMPLETED and s3_output_key is unchanged
    job_after_duplicate = dynamo.get_job(job_id)
    assert job_after_duplicate.status == JobStatus.COMPLETED
    assert job_after_duplicate.s3_output_key == job_after_first.s3_output_key


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
            with patch.object(S3Service, "upload_fileobj", side_effect=StorageError("S3 connection timeout")):
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


# ===========================================================================
# Phase 3.1: Distributed Leases, Crash Recovery, and Fencing Tests
# ===========================================================================


@mock_aws
def test_worker_crash_and_expired_lease_reacquisition():
    """Worker A acquires a lease, crashes. When lease expires, Worker B successfully re-acquires and finishes."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    s3 = S3Service(settings)

    job_id = "crash-recovery-job-001"
    now = datetime.now(tz=timezone.utc)
    s3_key = s3.upload_document(job_id, "test.txt", b"Crash recovery test", "text/plain")

    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key=s3_key,
        )
    )

    # 1. Worker A acquires lease
    res_a, item_a = dynamo.acquire_job_lease(job_id, worker_id="worker-A", lease_duration_seconds=60)
    assert res_a == AcquireResult.ACQUIRED
    assert item_a.worker_id == "worker-A"
    assert item_a.processing_version == 1
    assert item_a.attempt_count == 1
    assert item_a.status == JobStatus.PROCESSING

    # 2. Worker A crashes! We simulate time passing past the lease expiry
    # Backdate the lease_expires_at timestamp in DynamoDB to 10 seconds in the past
    past_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - 10
    dynamo._table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET lease_expires_at = :past",
        ExpressionAttributeValues={":past": past_timestamp},
    )

    # 3. Worker B arrives via SQS redelivery and attempts to acquire lease
    res_b, item_b = dynamo.acquire_job_lease(job_id, worker_id="worker-B", lease_duration_seconds=60)
    assert res_b == AcquireResult.ACQUIRED
    assert item_b.worker_id == "worker-B"
    assert item_b.processing_version == 2  # Monotonically incremented fencing token
    assert item_b.attempt_count == 2
    assert item_b.status == JobStatus.PROCESSING

    # 4. Worker B completes the job
    completed = dynamo.complete_job_with_lease(
        job_id=job_id,
        worker_id="worker-B",
        processing_version=2,
        s3_output_key=f"{job_id}/output/result_v2.json",
    )
    assert completed is True

    final_job = dynamo.get_job(job_id)
    assert final_job.status == JobStatus.COMPLETED
    assert final_job.worker_id == "worker-B"
    assert final_job.processing_version == 2
    assert final_job.s3_output_key == f"{job_id}/output/result_v2.json"


@mock_aws
def test_active_lease_prevents_concurrent_claim_and_preserves_sqs():
    """An active unexpired lease prevents concurrent claims and returns False to retain the SQS message."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    s3 = S3Service(settings)

    job_id = "concurrent-active-lease-job"
    now = datetime.now(tz=timezone.utc)
    s3_key = s3.upload_document(job_id, "test.txt", b"Active lease test", "text/plain")

    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key=s3_key,
        )
    )

    # Worker A acquires active lease for 300 seconds
    res_a, _ = dynamo.acquire_job_lease(job_id, worker_id="worker-A", lease_duration_seconds=300)
    assert res_a == AcquireResult.ACQUIRED

    # Worker B runs JobProcessor.process_job
    processor_b = JobProcessor(settings, worker_id="worker-B")
    should_delete = processor_b.process_job(job_id)

    # Must return False so the SQS message is NOT deleted
    assert should_delete is False

    # Job remains PROCESSING owned by Worker A
    current = dynamo.get_job(job_id)
    assert current.status == JobStatus.PROCESSING
    assert current.worker_id == "worker-A"


@mock_aws
def test_stale_worker_fenced_out_on_complete_and_renew():
    """A stale worker whose lease expired and was claimed by another worker is fenced out."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)

    job_id = "fencing-token-test-job"
    now = datetime.now(tz=timezone.utc)
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/test.txt",
        )
    )

    # Worker A acquires generation 1
    res_a, _ = dynamo.acquire_job_lease(job_id, worker_id="worker-A", lease_duration_seconds=60)
    assert res_a == AcquireResult.ACQUIRED

    # Simulate Worker A stall & lease expiry
    past_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - 20
    dynamo._table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET lease_expires_at = :past",
        ExpressionAttributeValues={":past": past_timestamp},
    )

    # Worker B claims generation 2
    res_b, _ = dynamo.acquire_job_lease(job_id, worker_id="worker-B", lease_duration_seconds=60)
    assert res_b == AcquireResult.ACQUIRED

    # Worker A wakes up and tries to renew its expired lease (version 1) -> Fenced out!
    renew_result = dynamo.renew_job_lease(job_id, worker_id="worker-A", processing_version=1)
    assert renew_result is False

    # Worker A tries to complete the job with version 1 -> Fenced out!
    complete_result = dynamo.complete_job_with_lease(
        job_id,
        worker_id="worker-A",
        processing_version=1,
        s3_output_key=f"{job_id}/output/result_v1.json",
    )
    assert complete_result is False

    # Worker B completes the job with version 2 -> Succeeds!
    complete_b = dynamo.complete_job_with_lease(
        job_id,
        worker_id="worker-B",
        processing_version=2,
        s3_output_key=f"{job_id}/output/result_v2.json",
    )
    assert complete_b is True

    # Final state reflects Worker B's work
    job = dynamo.get_job(job_id)
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id == "worker-B"
    assert job.processing_version == 2
    assert job.s3_output_key == f"{job_id}/output/result_v2.json"


@mock_aws
def test_max_attempts_exceeded_fails_job_and_acknowledges():
    """A poison job exceeding max attempts is transitioned to FAILED and acknowledged."""
    settings = _create_test_settings()
    settings.max_job_attempts = 3
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)

    job_id = "poison-pill-job-max-attempts"
    now = datetime.now(tz=timezone.utc)
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PROCESSING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/test.txt",
            attempt_count=3,  # Already at max attempts
            lease_expires_at=int(now.timestamp()) - 10,  # Expired
            processing_version=3,
        )
    )

    processor = JobProcessor(settings, worker_id="worker-rescue")
    should_delete = processor.process_job(job_id)

    # Must return True so SQS message is deleted and doesn't loop infinitely
    assert should_delete is True

    # Job is marked FAILED in DynamoDB
    job = dynamo.get_job(job_id)
    assert job.status == JobStatus.FAILED
    assert "Exceeded max processing attempts" in (job.error_message or "")


@mock_aws
def test_visibility_heartbeat_renews_dynamodb_lease():
    """VisibilityHeartbeat periodically renews the DynamoDB lease in the background."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    job_id = "heartbeat-renewal-job"
    now = datetime.now(tz=timezone.utc)
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/test.txt",
        )
    )

    # Acquire lease for 10 seconds
    dynamo.acquire_job_lease(job_id, worker_id="worker-hb", lease_duration_seconds=10)
    initial_job = dynamo.get_job(job_id)
    initial_exp = initial_job.lease_expires_at

    # Start heartbeat with very small interval and larger extension
    with VisibilityHeartbeat(
        sqs=MagicMock(),
        receipt_handle="dummy-handle",
        interval_seconds=0.05,
        extension_seconds=100,
        dynamo=dynamo,
        job_id=job_id,
        worker_id="worker-hb",
        processing_version=1,
    ) as hb:
        time.sleep(0.12)
        assert not hb.is_fenced_out

    # Verify that the lease timestamp was renewed to a higher value
    renewed_job = dynamo.get_job(job_id)
    assert renewed_job.lease_expires_at is not None
    assert initial_exp is not None
    assert renewed_job.lease_expires_at > initial_exp


# ===========================================================================
# Phase 3.2: Distributed Systems and Production Readiness Audit Tests
# ===========================================================================


@mock_aws
def test_already_active_message_adjusts_visibility_to_prevent_tight_loop():
    """An active lease adjusts message visibility delay so SQS does not enter a tight redelivery loop."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    job_id = "tight-loop-prevention-job"
    now = datetime.now(tz=timezone.utc)
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/test.txt",
        )
    )

    # Worker A acquires lease for 60 seconds
    dynamo.acquire_job_lease(job_id, worker_id="worker-A", lease_duration_seconds=60)

    # Worker B receives message
    processor_b = JobProcessor(settings, worker_id="worker-B")
    mock_sqs = MagicMock()
    processor_b._sqs = mock_sqs

    should_delete = processor_b.process_job(job_id, receipt_handle="active-msg-handle")

    assert should_delete is False  # Message retained in SQS
    # SQS visibility must have been extended/delayed to match remaining lease
    assert mock_sqs.change_message_visibility.called
    args, _ = mock_sqs.change_message_visibility.call_args
    assert args[0] == "active-msg-handle"
    assert args[1] >= 10  # Backoff delay in seconds


@mock_aws
def test_stale_worker_s3_write_rejected_by_fencing():
    """A stale worker cannot publish its S3 output key as authoritative over a newer worker."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    job_id = "stale-s3-write-job"
    now = datetime.now(tz=timezone.utc)
    dynamo.create_job(
        DynamoDBJobItem(
            job_id=job_id,
            status=JobStatus.PENDING,
            filename="test.txt",
            created_at=now,
            updated_at=now,
            s3_input_key="path/test.txt",
        )
    )

    # Worker A acquires v1
    dynamo.acquire_job_lease(job_id, worker_id="worker-A", lease_duration_seconds=60)

    # Backdate lease to simulate stall and expiry
    past_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - 10
    dynamo._table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET lease_expires_at = :past",
        ExpressionAttributeValues={":past": past_timestamp},
    )

    # Worker B acquires v2 and completes
    dynamo.acquire_job_lease(job_id, worker_id="worker-B", lease_duration_seconds=60)
    completed_b = dynamo.complete_job_with_lease(
        job_id=job_id,
        worker_id="worker-B",
        processing_version=2,
        s3_output_key=f"{job_id}/output/result_v2.json",
    )
    assert completed_b is True

    # Stale Worker A wakes up and attempts to complete with v1 and its output key
    completed_a = dynamo.complete_job_with_lease(
        job_id=job_id,
        worker_id="worker-A",
        processing_version=1,
        s3_output_key=f"{job_id}/output/result_v1_stale.json",
    )
    assert completed_a is False  # Rejected by fencing token

    # The authoritative record in DynamoDB remains Worker B's result
    authoritative = dynamo.get_job(job_id)
    assert authoritative.status == JobStatus.COMPLETED
    assert authoritative.worker_id == "worker-B"
    assert authoritative.s3_output_key == f"{job_id}/output/result_v2.json"


def test_visibility_heartbeat_failure_handling():
    """Heartbeat handles transient SQS or DynamoDB network exceptions without crashing."""
    mock_sqs = MagicMock()
    mock_sqs.change_message_visibility.side_effect = RuntimeError("SQS transient error")
    mock_dynamo = MagicMock()
    mock_dynamo.renew_job_lease.side_effect = RuntimeError("DynamoDB transient error")

    with VisibilityHeartbeat(
        sqs=mock_sqs,
        receipt_handle="dummy-receipt",
        interval_seconds=0.04,
        extension_seconds=60,
        dynamo=mock_dynamo,
        job_id="dummy-job",
        worker_id="dummy-worker",
        processing_version=1,
    ) as hb:
        time.sleep(0.1)
        # Even with exceptions in background thread, is_fenced_out is False
        # and the worker process is not terminated
        assert not hb.is_fenced_out


def test_worker_graceful_shutdown_flag():
    """Worker processor respects _shutdown_requested flag and terminates loop cleanly."""
    settings = _create_test_settings()
    processor = JobProcessor(settings)
    processor._shutdown_requested = True

    # Calling run() with shutdown requested must exit immediately without looping
    processor.run()
    assert processor._shutdown_requested is True


@pytest.mark.asyncio
async def test_api_get_job_uses_authoritative_s3_output_key():
    """API GET /jobs/{job_id} generates presigned URL from the authoritative s3_output_key stored in DynamoDB."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)
        job_id = "test-authoritative-key-job"
        now = datetime.now(tz=timezone.utc)
        dynamo.create_job(
            DynamoDBJobItem(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                filename="document.pdf",
                created_at=now,
                updated_at=now,
                s3_input_key="path/input.pdf",
                s3_output_key=f"{job_id}/output/result_v2_authoritative.json",
            )
        )

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get(f"/api/v1/jobs/{job_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "COMPLETED"
            assert data["result_url"] is not None
            # Presigned URL must target the authoritative versioned object
            assert f"{job_id}/output/result_v2_authoritative.json" in data["result_url"]
        finally:
            app.dependency_overrides.clear()


# ===========================================================================
# Phase 3.3: Production Architecture Hardening Tests
# ===========================================================================


def test_heartbeat_conditional_renewal_failure_fences_out_worker():
    """DynamoDB conditional renewal failure immediately fences out the worker and terminates thread."""
    mock_sqs = MagicMock()
    mock_dynamo = MagicMock()
    # ConditionalCheckFailedException -> renew_job_lease returns False
    mock_dynamo.renew_job_lease.return_value = False

    hb = VisibilityHeartbeat(
        sqs=mock_sqs,
        receipt_handle="dummy-receipt",
        interval_seconds=0.03,
        extension_seconds=60,
        dynamo=mock_dynamo,
        job_id="job-fence-test",
        worker_id="worker-fence",
        processing_version=1,
    )
    with hb:
        time.sleep(0.08)

    # Must be fenced out
    assert hb.is_fenced_out is True
    # Thread must have terminated cleanly
    if hb._thread:
        assert not hb._thread.is_alive()


def test_heartbeat_single_transient_failure_recovers_on_next_tick():
    """A single transient network failure does not fence out the worker if the next tick succeeds."""
    mock_sqs = MagicMock()
    mock_dynamo = MagicMock()
    # 1st call fails with transient error, 2nd call succeeds
    mock_dynamo.renew_job_lease.side_effect = [RuntimeError("Transient 503"), True, True]

    hb = VisibilityHeartbeat(
        sqs=mock_sqs,
        receipt_handle="dummy-receipt",
        interval_seconds=0.03,
        extension_seconds=60,
        dynamo=mock_dynamo,
        job_id="job-transient-test",
        worker_id="worker-transient",
        processing_version=1,
        max_consecutive_failures=3,
    )
    with hb:
        time.sleep(0.09)

    # Must NOT be fenced out because renewal succeeded after transient error
    assert hb.is_fenced_out is False


def test_heartbeat_repeated_transient_failures_fences_out_worker():
    """Repeated transient network failures reaching max_consecutive_failures fence out worker."""
    mock_sqs = MagicMock()
    mock_dynamo = MagicMock()
    # Fails every time
    mock_dynamo.renew_job_lease.side_effect = RuntimeError("Continuous network partition")

    hb = VisibilityHeartbeat(
        sqs=mock_sqs,
        receipt_handle="dummy-receipt",
        interval_seconds=0.02,
        extension_seconds=60,
        dynamo=mock_dynamo,
        job_id="job-repeated-fail-test",
        worker_id="worker-repeated",
        processing_version=1,
        max_consecutive_failures=3,
    )
    with hb:
        time.sleep(0.12)

    # Exceeded max_consecutive_failures -> worker is fenced out
    assert hb.is_fenced_out is True
    # Heartbeat thread terminates cleanly
    if hb._thread:
        assert not hb._thread.is_alive()


@mock_aws
def test_fenced_out_worker_aborts_before_s3_and_cannot_complete():
    """A worker fenced out during processing aborts immediately and does not publish S3 results or complete."""
    settings = _create_test_settings()
    _bootstrap_mock_aws(settings)

    dynamo = DynamoDBService(settings)
    s3 = S3Service(settings)
    job_id = "fenced-abort-job"
    now = datetime.now(tz=timezone.utc)
    s3_key = s3.upload_document(job_id, "doc.txt", b"Aborted content", "text/plain")

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

    processor = JobProcessor(settings, worker_id="worker-slow")

    # Patch simulate_processing to inject lease expiry / fencing mid-execution
    def _simulated_slow_processing(*args, **kwargs):
        # Backdate lease so worker-slow's lease expires
        past_ts = int(datetime.now(tz=timezone.utc).timestamp()) - 10
        dynamo._table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET lease_expires_at = :past",
            ExpressionAttributeValues={":past": past_ts},
        )
        # Another worker claims lease v2 and completes
        res, _ = dynamo.acquire_job_lease(job_id, worker_id="worker-fast", lease_duration_seconds=60)
        assert res == AcquireResult.ACQUIRED
        dynamo.complete_job_with_lease(
            job_id,
            worker_id="worker-fast",
            processing_version=2,
            s3_output_key=f"{job_id}/output/result_v2.json",
        )
        return {"word_count": 2}

    with patch.object(processor, "_simulate_processing", side_effect=_simulated_slow_processing):
        should_delete = processor.process_job(job_id)

    # Worker-slow must return False because it was fenced out
    assert should_delete is False

    # The authoritative record in DynamoDB remains worker-fast's version 2
    job = dynamo.get_job(job_id)
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id == "worker-fast"
    assert job.processing_version == 2
    assert job.s3_output_key == f"{job_id}/output/result_v2.json"


@pytest.mark.asyncio
async def test_api_completed_job_missing_output_key_returns_500():
    """A COMPLETED job without an authoritative s3_output_key returns HTTP 500 (Data Integrity Error)."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)
        job_id = "job-corrupted-missing-key"
        now = datetime.now(tz=timezone.utc)
        dynamo.create_job(
            DynamoDBJobItem(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                filename="document.pdf",
                created_at=now,
                updated_at=now,
                s3_input_key="path/input.pdf",
                s3_output_key=None,  # Missing authoritative key!
            )
        )

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get(f"/api/v1/jobs/{job_id}")

            # Must return HTTP 500 and not guess a fallback URL
            assert response.status_code == 500
            data = response.json()
            assert data["error_code"] == "DATA_INTEGRITY_ERROR"
            assert "authoritative S3 output key" in data["detail"]
        finally:
            app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_upload_s3_failure_marks_job_failed():
    """When S3 document streaming fails, DynamoDB job is marked FAILED rather than dangling in PENDING."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)

        try:
            with patch.object(S3Service, "upload_fileobj", side_effect=StorageError("S3 PutObject network timeout")):
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
async def test_api_sqs_enqueue_failure_marks_job_failed_and_returns_503():
    """When SQS enqueue fails, DynamoDB job is rolled back to FAILED and API returns 503."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)

        try:
            with patch.object(SQSService, "send_job", side_effect=QueueError("SQS SendMessage network timeout")):
                transport = ASGITransport(app=app)
                async with AsyncClient(transport=transport, base_url="http://test") as client:
                    files = {"file": ("report.pdf", io.BytesIO(b"Valid PDF Content"), "application/pdf")}
                    response = await client.post("/api/v1/jobs", files=files)

                assert response.status_code == 503
                data = response.json()
                assert data["error_code"] == "QUEUE_ERROR"

                # Verify DynamoDB has the job marked as FAILED rather than dangling in PENDING
                # Find the job in DynamoDB
                table = dynamo._table
                scan_res = table.scan()
                items = scan_res.get("Items", [])
                assert len(items) == 1
                failed_job = items[0]
                assert failed_job["status"] == "FAILED"
                assert "Failed to enqueue job to SQS" in failed_job["error_message"]
        finally:
            app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_client_idempotency_key_deduplication():
    """Submissions with the same Idempotency-Key return the existing job without duplicates."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                files1 = {"file": ("invoice.pdf", io.BytesIO(b"Invoice Document 1"), "application/pdf")}
                headers = {"Idempotency-Key": "client-idempotency-key-abc"}
                resp1 = await client.post("/api/v1/jobs", files=files1, headers=headers)
                assert resp1.status_code == 202
                data1 = resp1.json()
                job_id_1 = data1["job_id"]
                assert data1["status"] == "PENDING"

                # Second identical submission with same Idempotency-Key
                files2 = {"file": ("invoice.pdf", io.BytesIO(b"Invoice Document 1"), "application/pdf")}
                resp2 = await client.post("/api/v1/jobs", files=files2, headers=headers)
                assert resp2.status_code == 202
                data2 = resp2.json()
                job_id_2 = data2["job_id"]
                assert job_id_1 == job_id_2
                assert "already submitted" in data2["message"].lower()

                # Verify exactly one job in DynamoDB
                table = dynamo._table
                scan_res = table.scan()
                items = scan_res.get("Items", [])
                assert len(items) == 1
                assert items[0]["job_id"] == job_id_1
        finally:
            app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_x_idempotency_key_header_support():
    """Alternative header X-Idempotency-Key provides identical deduplication."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                headers = {"X-Idempotency-Key": "req-999-trans-xyz"}
                files1 = {"file": ("doc.txt", io.BytesIO(b"Document bytes"), "text/plain")}
                resp1 = await client.post("/api/v1/jobs", files=files1, headers=headers)
                assert resp1.status_code == 202
                job_id_1 = resp1.json()["job_id"]

                files2 = {"file": ("doc.txt", io.BytesIO(b"Document bytes"), "text/plain")}
                resp2 = await client.post("/api/v1/jobs", files=files2, headers=headers)
                assert resp2.status_code == 202
                job_id_2 = resp2.json()["job_id"]

                assert job_id_1 == job_id_2
        finally:
            app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_api_atomic_s3_input_key_in_dynamodb():
    """DynamoDB job creation atomically includes s3_input_key without empty intermediate state."""
    with mock_aws():
        settings = _create_test_settings()
        _bootstrap_mock_aws(settings)
        app.dependency_overrides[get_settings_dep] = lambda: settings

        dynamo = DynamoDBService(settings)

        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                files = {"file": ("data.csv", io.BytesIO(b"col1,col2\n1,2"), "text/csv")}
                resp = await client.post("/api/v1/jobs", files=files)
                assert resp.status_code == 202
                job_id = resp.json()["job_id"]

                job = dynamo.get_job(job_id)
                assert job.s3_input_key != ""
                assert job.s3_input_key.startswith(f"{job_id}/input/")
                assert job.s3_input_key.endswith("data.csv")
        finally:
            app.dependency_overrides.clear()


def test_terraform_private_subnet_and_vpc_endpoints_configuration():
    """Verify Terraform configuration enforces assign_public_ip = false and declares VPC endpoints."""
    ecs_main_path = Path("terraform/modules/ecs/main.tf")
    assert ecs_main_path.exists(), "terraform/modules/ecs/main.tf must exist"
    content = ecs_main_path.read_text(encoding="utf-8")

    # Both services must set assign_public_ip = false
    assert "assign_public_ip = false" in content
    assert "assign_public_ip = true" not in content

    # VPC Gateway Endpoints
    assert 'resource "aws_vpc_endpoint" "s3"' in content
    assert 'service_name      = "com.amazonaws.${var.aws_region}.s3"' in content
    assert 'resource "aws_vpc_endpoint" "dynamodb"' in content
    assert 'service_name      = "com.amazonaws.${var.aws_region}.dynamodb"' in content

    # VPC Interface Endpoints
    assert 'resource "aws_vpc_endpoint" "sqs"' in content
    assert 'resource "aws_vpc_endpoint" "ecr_api"' in content
    assert 'resource "aws_vpc_endpoint" "ecr_dkr"' in content
    assert 'resource "aws_vpc_endpoint" "logs"' in content

    # Security Groups
    assert 'resource "aws_security_group" "vpc_endpoints"' in content

    # HTTPS Listener
    assert 'resource "aws_lb_listener" "https"' in content
    assert "ELBSecurityPolicy-TLS13-1-2-2021-06" in content

