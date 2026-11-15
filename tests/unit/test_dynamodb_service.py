"""
Unit tests for DynamoDBService (app/services/dynamodb_service.py).

All tests run under moto's ``@mock_aws`` decorator so no real AWS calls are made.
"""

from __future__ import annotations

from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from app.core.config import Settings
from app.core.exceptions import JobAlreadyExistsError, JobNotFoundError
from app.models.job import DynamoDBJobItem, JobStatus
from app.services.dynamodb_service import DynamoDBService


def _setup_dynamodb():
    """Helper to configure settings and create the DynamoDB table inside @mock_aws context."""
    resource = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    table = resource.create_table(
        TableName="test-jobs",
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    settings = Settings(
        aws_default_region="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_endpoint_url=None,
        dynamodb_table_name="test-jobs",
        s3_bucket_name="documents",
        sqs_queue_url="http://moto-sqs/queue/jobs-queue",
        log_level="WARNING",
        environment="test",
    )
    return settings, DynamoDBService(settings)


def _make_item(job_id: str = "job-001", status: JobStatus = JobStatus.PENDING) -> DynamoDBJobItem:
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return DynamoDBJobItem(
        job_id=job_id,
        status=status,
        filename="test.pdf",
        created_at=now,
        updated_at=now,
        s3_input_key=f"{job_id}/input/test.pdf",
        result_url=None,
        error_message=None,
    )


@mock_aws
def test_create_job_success():
    """create a job, then get it back, verify status=PENDING."""
    settings, service = _setup_dynamodb()
    item = _make_item("job-create-1")

    service.create_job(item)

    fetched = service.get_job("job-create-1")
    assert fetched.job_id == "job-create-1"
    assert fetched.status == JobStatus.PENDING
    assert fetched.filename == "test.pdf"


@mock_aws
def test_create_job_idempotency():
    """creating same job_id twice raises JobAlreadyExistsError."""
    settings, service = _setup_dynamodb()
    item = _make_item("job-dup-1")

    service.create_job(item)

    with pytest.raises(JobAlreadyExistsError):
        service.create_job(item)


@mock_aws
def test_get_job_not_found():
    """getting nonexistent job_id raises JobNotFoundError."""
    settings, service = _setup_dynamodb()

    with pytest.raises(JobNotFoundError):
        service.get_job("nonexistent-job-id")


@mock_aws
def test_update_job_status_to_processing():
    """update to PROCESSING, verify status is updated in DynamoDB."""
    settings, service = _setup_dynamodb()
    item = _make_item("job-proc-1")
    service.create_job(item)

    service.update_job_status("job-proc-1", JobStatus.PROCESSING)

    fetched = service.get_job("job-proc-1")
    assert fetched.status == JobStatus.PROCESSING


@mock_aws
def test_update_job_status_to_completed():
    """update to COMPLETED with result_url, verify status and result_url."""
    settings, service = _setup_dynamodb()
    item = _make_item("job-comp-1")
    service.create_job(item)

    result_url = "https://documents.s3.amazonaws.com/job-comp-1/output/result.json"
    service.update_job_status("job-comp-1", JobStatus.COMPLETED, result_url=result_url)

    fetched = service.get_job("job-comp-1")
    assert fetched.status == JobStatus.COMPLETED
    assert fetched.result_url == result_url


@mock_aws
def test_update_job_status_to_failed():
    """update to FAILED with error_message, verify status and error_message."""
    settings, service = _setup_dynamodb()
    item = _make_item("job-fail-1")
    service.create_job(item)

    service.update_job_status(
        "job-fail-1",
        JobStatus.FAILED,
        error_message="Document corrupted",
    )

    fetched = service.get_job("job-fail-1")
    assert fetched.status == JobStatus.FAILED
    assert fetched.error_message == "Document corrupted"
