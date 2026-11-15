"""
Unit tests for SQSService (app/services/sqs_service.py).

All tests run under moto's ``@mock_aws`` decorator so no real AWS calls are made.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from app.core.config import Settings
from app.services.sqs_service import SQSService


def _setup_sqs():
    """Helper to configure settings and create a test queue inside @mock_aws context."""
    client = boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    resp = client.create_queue(QueueName="test-jobs-queue")
    queue_url = resp["QueueUrl"]

    settings = Settings(
        aws_default_region="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_endpoint_url=None,
        dynamodb_table_name="jobs",
        s3_bucket_name="documents",
        sqs_queue_url=queue_url,
        log_level="WARNING",
        environment="test",
    )
    return settings, SQSService(settings)


@mock_aws
def test_send_job_returns_message_id():
    """send_job should enqueue message and return SQS MessageId."""
    settings, service = _setup_sqs()
    message_id = service.send_job("job-123")
    assert isinstance(message_id, str)
    assert len(message_id) > 0


@mock_aws
def test_receive_messages_returns_job():
    """send then receive should retrieve the matching job_id and receipt_handle."""
    settings, service = _setup_sqs()
    service.send_job("job-abc-456")

    messages = service.receive_messages(max_messages=1, wait_seconds=1)
    assert len(messages) == 1
    assert messages[0]["job_id"] == "job-abc-456"
    assert "receipt_handle" in messages[0]
    assert len(messages[0]["receipt_handle"]) > 0


@mock_aws
def test_delete_message_removes_from_queue():
    """send, receive, delete, receive again should result in an empty list."""
    settings, service = _setup_sqs()
    service.send_job("job-to-delete")

    messages = service.receive_messages(max_messages=1, wait_seconds=1)
    assert len(messages) == 1
    receipt_handle = messages[0]["receipt_handle"]

    service.delete_message(receipt_handle)

    second_messages = service.receive_messages(max_messages=1, wait_seconds=1)
    assert len(second_messages) == 0
