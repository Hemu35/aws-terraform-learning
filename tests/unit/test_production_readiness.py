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
