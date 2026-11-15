"""
Unit tests for Pydantic models in app/models/job.py.

Covers:
- JobStatus enum values and string coercion
- DynamoDBJobItem serialisation round-trip via to_dynamodb_item / from_dynamodb_item
- JobCreate field validation
- Datetime precision preserved through serialise → deserialise cycle
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models.job import DynamoDBJobItem, JobCreate, JobStatus, JobSubmitResponse


# ---------------------------------------------------------------------------
# JobStatus enum
# ---------------------------------------------------------------------------


class TestJobStatus:
    def test_pending_value(self):
        assert JobStatus.PENDING == "PENDING"

    def test_processing_value(self):
        assert JobStatus.PROCESSING == "PROCESSING"

    def test_completed_value(self):
        assert JobStatus.COMPLETED == "COMPLETED"

    def test_failed_value(self):
        assert JobStatus.FAILED == "FAILED"

    def test_is_str_enum(self):
        """JobStatus should be usable wherever a str is expected."""
        assert isinstance(JobStatus.PENDING, str)

    def test_coerce_from_string(self):
        assert JobStatus("PENDING") is JobStatus.PENDING

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            JobStatus("UNKNOWN")


# ---------------------------------------------------------------------------
# JobCreate validation
# ---------------------------------------------------------------------------


class TestJobCreate:
    def test_valid_minimal(self):
        job = JobCreate(filename="report.pdf")
        assert job.filename == "report.pdf"
        assert job.content_type == "application/octet-stream"

    def test_custom_content_type(self):
        job = JobCreate(filename="data.csv", content_type="text/csv")
        assert job.content_type == "text/csv"

    def test_missing_filename_raises(self):
        with pytest.raises(ValidationError):
            JobCreate()  # filename is required

    def test_empty_filename_accepted(self):
        """Pydantic does not enforce non-empty strings by default."""
        job = JobCreate(filename="")
        assert job.filename == ""


# ---------------------------------------------------------------------------
# DynamoDBJobItem serialisation round-trip
# ---------------------------------------------------------------------------


class TestDynamoDBJobItemSerialization:
    def _make_item(self, **overrides) -> DynamoDBJobItem:
        now = datetime(2024, 6, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)
        defaults = dict(
            job_id="job-abc-001",
            status=JobStatus.PENDING,
            filename="invoice.pdf",
            created_at=now,
            updated_at=now,
            s3_input_key="job-abc-001/input/invoice.pdf",
            result_url=None,
            error_message=None,
        )
        defaults.update(overrides)
        return DynamoDBJobItem(**defaults)

    def test_to_dynamodb_item_contains_all_keys(self):
        item = self._make_item()
        raw = item.to_dynamodb_item()
        expected_keys = {
            "job_id", "status", "filename", "created_at",
            "updated_at", "s3_input_key", "result_url", "error_message",
        }
        assert set(raw.keys()) == expected_keys

    def test_to_dynamodb_item_status_is_string(self):
        item = self._make_item(status=JobStatus.PROCESSING)
        raw = item.to_dynamodb_item()
        assert raw["status"] == "PROCESSING"
        assert isinstance(raw["status"], str)

    def test_to_dynamodb_item_datetime_is_isostring(self):
        now = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        item = self._make_item(created_at=now, updated_at=now)
        raw = item.to_dynamodb_item()
        # Must be a string, not a datetime object (DynamoDB rejects datetimes)
        assert isinstance(raw["created_at"], str)
        assert isinstance(raw["updated_at"], str)

    def test_round_trip_preserves_all_fields(self, sample_job_item):
        raw = sample_job_item.to_dynamodb_item()
        restored = DynamoDBJobItem.from_dynamodb_item(raw)

        assert restored.job_id == sample_job_item.job_id
        assert restored.status == sample_job_item.status
        assert restored.filename == sample_job_item.filename
        assert restored.s3_input_key == sample_job_item.s3_input_key
        assert restored.result_url == sample_job_item.result_url
        assert restored.error_message == sample_job_item.error_message

    def test_round_trip_preserves_datetime_precision(self):
        now = datetime(2024, 6, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)
        item = self._make_item(created_at=now, updated_at=now)
        raw = item.to_dynamodb_item()
        restored = DynamoDBJobItem.from_dynamodb_item(raw)

        assert restored.created_at == now
        assert restored.updated_at == now

    def test_round_trip_with_optional_fields(self):
        item = self._make_item(
            status=JobStatus.COMPLETED,
            result_url="https://example.com/result",
            error_message=None,
        )
        raw = item.to_dynamodb_item()
        restored = DynamoDBJobItem.from_dynamodb_item(raw)

        assert restored.result_url == "https://example.com/result"
        assert restored.error_message is None

    def test_round_trip_failed_status(self):
        item = self._make_item(
            status=JobStatus.FAILED,
            error_message="processing timed out",
        )
        raw = item.to_dynamodb_item()
        restored = DynamoDBJobItem.from_dynamodb_item(raw)

        assert restored.status == JobStatus.FAILED
        assert restored.error_message == "processing timed out"


# ---------------------------------------------------------------------------
# JobSubmitResponse
# ---------------------------------------------------------------------------


class TestJobSubmitResponse:
    def test_valid(self):
        resp = JobSubmitResponse(
            job_id="job-1",
            status=JobStatus.PENDING,
            message="submitted",
        )
        assert resp.job_id == "job-1"
        assert resp.status == JobStatus.PENDING
