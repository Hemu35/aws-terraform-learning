"""
Pytest fixtures shared across all unit tests.

Key design notes:
- ``aws_settings`` uses no endpoint_url so moto intercepts boto3 calls
  directly — no real AWS or LocalStack needed.
- Each AWS fixture (s3_bucket, dynamodb_table, sqs_queue) is wrapped in
  ``mock_aws`` so resources are created fresh per test and torn down cleanly.
- ``mock_aws`` must be active *before* any boto3 client / resource is created;
  wrapping the fixture body is the safest pattern.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from app.core.config import Settings
from app.models.job import DynamoDBJobItem, JobStatus

# ---------------------------------------------------------------------------
# Silence pydantic-settings .env loading during tests
# ---------------------------------------------------------------------------
# Prevent Settings from picking up a real .env file from the repo root.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("SQS_QUEUE_URL", "http://moto-sqs/queue/jobs-queue")


# ---------------------------------------------------------------------------
# Settings fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def aws_settings() -> Settings:
    """Return a Settings instance with fake credentials and no endpoint_url.

    moto intercepts boto3 at the botocore level, so endpoint_url must NOT
    be set (setting it to a real LocalStack URL would bypass moto).
    """
    return Settings(
        aws_default_region="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_endpoint_url=None,  # let moto intercept
        dynamodb_table_name="jobs",
        s3_bucket_name="documents",
        sqs_queue_url="http://moto-sqs/queue/jobs-queue",
        log_level="WARNING",
        environment="test",
    )


# ---------------------------------------------------------------------------
# S3 fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_bucket(aws_settings: Settings):
    """Create a mocked S3 bucket for the duration of one test."""
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name=aws_settings.aws_default_region,
            aws_access_key_id=aws_settings.aws_access_key_id,
            aws_secret_access_key=aws_settings.aws_secret_access_key,
        )
        client.create_bucket(Bucket=aws_settings.s3_bucket_name)
        yield client


# ---------------------------------------------------------------------------
# DynamoDB fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def dynamodb_table(aws_settings: Settings):
    """Create a mocked DynamoDB table with PAY_PER_REQUEST billing."""
    with mock_aws():
        resource = boto3.resource(
            "dynamodb",
            region_name=aws_settings.aws_default_region,
            aws_access_key_id=aws_settings.aws_access_key_id,
            aws_secret_access_key=aws_settings.aws_secret_access_key,
        )
        table = resource.create_table(
            TableName=aws_settings.dynamodb_table_name,
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table


# ---------------------------------------------------------------------------
# SQS fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def sqs_queue(aws_settings: Settings):
    """Create a mocked SQS queue and return its URL."""
    with mock_aws():
        client = boto3.client(
            "sqs",
            region_name=aws_settings.aws_default_region,
            aws_access_key_id=aws_settings.aws_access_key_id,
            aws_secret_access_key=aws_settings.aws_secret_access_key,
        )
        response = client.create_queue(QueueName="jobs-queue")
        queue_url: str = response["QueueUrl"]
        yield queue_url


# ---------------------------------------------------------------------------
# Sample domain-model fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_job_item() -> DynamoDBJobItem:
    """Return a fully populated DynamoDBJobItem in PENDING status."""
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return DynamoDBJobItem(
        job_id="test-job-123",
        status=JobStatus.PENDING,
        filename="sample.pdf",
        created_at=now,
        updated_at=now,
        s3_input_key="test-job-123/input/sample.pdf",
        result_url=None,
        error_message=None,
    )
