"""
Unit tests for S3Service (app/services/s3_service.py).

All tests run under moto's ``@mock_aws`` decorator so no real AWS calls are made.
Each test creates its own S3Service instance against the mocked endpoint.
"""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from app.core.config import Settings
from app.services.s3_service import S3Service


def _make_settings() -> Settings:
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
    )


def _create_bucket(settings: Settings) -> None:
    """Helper: create the S3 bucket inside an already-active mock context."""
    boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    ).create_bucket(Bucket=settings.s3_bucket_name)


# ---------------------------------------------------------------------------
# upload_document
# ---------------------------------------------------------------------------


@mock_aws
def test_upload_document_success():
    """Uploaded bytes should exist in the mocked S3 bucket."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    service.upload_document(
        job_id="job-1",
        filename="report.pdf",
        content=b"%PDF-binary-content",
        content_type="application/pdf",
    )

    # Verify the object is reachable via a raw boto3 client
    client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    response = client.get_object(Bucket=settings.s3_bucket_name, Key="job-1/input/report.pdf")
    assert response["Body"].read() == b"%PDF-binary-content"


@mock_aws
def test_upload_document_returns_key():
    """Return value must follow the ``{job_id}/input/{filename}`` convention."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    key = service.upload_document(
        job_id="job-42",
        filename="data.csv",
        content=b"col1,col2\n1,2",
        content_type="text/csv",
    )

    assert key == "job-42/input/data.csv"


@mock_aws
def test_upload_document_stores_content_type():
    """ContentType metadata should be stored on the S3 object."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    service.upload_document(
        job_id="job-ct",
        filename="image.png",
        content=b"\x89PNG",
        content_type="image/png",
    )

    client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    head = client.head_object(Bucket=settings.s3_bucket_name, Key="job-ct/input/image.png")
    assert head["ContentType"] == "image/png"


# ---------------------------------------------------------------------------
# upload_result
# ---------------------------------------------------------------------------


@mock_aws
def test_upload_result():
    """Result dict should be stored as JSON at ``{job_id}/output/result.json``."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    result = {"pages": 5, "words": 1200, "language": "en"}
    service.upload_result(job_id="job-res", result=result)

    client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    response = client.get_object(Bucket=settings.s3_bucket_name, Key="job-res/output/result.json")
    stored = json.loads(response["Body"].read())
    assert stored == result


@mock_aws
def test_upload_result_returns_key():
    """upload_result should return the S3 key."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    key = service.upload_result(job_id="job-rk", result={"ok": True})
    assert key == "job-rk/output/result.json"


# ---------------------------------------------------------------------------
# get_result
# ---------------------------------------------------------------------------


@mock_aws
def test_get_result_returns_none_when_missing():
    """Requesting a result for a job that has no output should return None."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    result = service.get_result("nonexistent-job-id")
    assert result is None


@mock_aws
def test_get_result_returns_data():
    """After uploading a result, get_result should return the raw bytes."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    payload = {"status": "done", "score": 0.99}
    service.upload_result(job_id="job-gr", result=payload)

    raw = service.get_result("job-gr")
    assert raw is not None
    assert json.loads(raw) == payload


# ---------------------------------------------------------------------------
# generate_presigned_url
# ---------------------------------------------------------------------------


@mock_aws
def test_generate_presigned_url_returns_string():
    """Presigned URL should be a non-empty string."""
    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    # Upload an object first so the key exists
    service.upload_document(
        job_id="job-pu",
        filename="doc.txt",
        content=b"hello",
        content_type="text/plain",
    )

    url = service.generate_presigned_url("job-pu/input/doc.txt")
    assert isinstance(url, str)
    assert len(url) > 0


@mock_aws
def test_upload_fileobj_success():
    """Streaming upload via upload_fileobj stores bytes in mocked S3 bucket."""
    import io

    settings = _make_settings()
    _create_bucket(settings)
    service = S3Service(settings)

    stream = io.BytesIO(b"Streamed binary content")
    s3_key = service.upload_fileobj(
        job_id="job-stream",
        filename="stream.pdf",
        fileobj=stream,
        content_type="application/pdf",
        size_bytes=len(stream.getvalue()),
    )
    assert s3_key == "job-stream/input/stream.pdf"

    client = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    response = client.get_object(Bucket=settings.s3_bucket_name, Key=s3_key)
    assert response["Body"].read() == b"Streamed binary content"
