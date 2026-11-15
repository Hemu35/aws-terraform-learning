"""Pydantic v2 models for document-processing jobs.

These models are shared across the API layer, service layer, and worker so
that the whole codebase speaks the same types.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Lifecycle states of a document-processing job."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class AcquireResult(str, Enum):
    """Outcome of attempting to acquire a job processing lease."""

    ACQUIRED = "ACQUIRED"                      # Successfully claimed / re-acquired lease
    ALREADY_COMPLETED = "ALREADY_COMPLETED"    # Job finished; safe to delete SQS message
    ALREADY_ACTIVE = "ALREADY_ACTIVE"          # Another worker holds active lease; keep message in SQS
    MAX_ATTEMPTS_EXCEEDED = "MAX_ATTEMPTS_EXCEEDED"  # Retry limit hit; send to DLQ / fail
    NOT_FOUND = "NOT_FOUND"                    # Job record does not exist


# ---------------------------------------------------------------------------
# API request / response models
# ---------------------------------------------------------------------------


class JobCreate(BaseModel):
    """Payload for creating a new job (used internally by the API handler)."""

    filename: str
    content_type: str = "application/octet-stream"


class JobResponse(BaseModel):
    """Full job representation returned to callers."""

    job_id: str
    status: JobStatus
    filename: str
    created_at: datetime
    updated_at: datetime
    result_url: Optional[str] = None
    error_message: Optional[str] = None


class JobSubmitResponse(BaseModel):
    """Slim acknowledgement returned immediately on job submission (HTTP 202)."""

    job_id: str
    status: JobStatus
    message: str


# ---------------------------------------------------------------------------
# DynamoDB persistence model
# ---------------------------------------------------------------------------


class DynamoDBJobItem(BaseModel):
    """Full item as stored in DynamoDB.

    Extends :class:`JobResponse` with the S3 keys of upload and output,
    plus an optional Unix epoch `expires_at` timestamp for DynamoDB TTL.
    """

    job_id: str
    status: JobStatus
    filename: str
    created_at: datetime
    updated_at: datetime
    s3_input_key: str
    s3_output_key: Optional[str] = None
    result_url: Optional[str] = None
    error_message: Optional[str] = None
    expires_at: Optional[int] = None
    worker_id: Optional[str] = None
    lease_expires_at: Optional[int] = None
    attempt_count: int = 0
    processing_version: int = 0

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dynamodb_item(self) -> dict:
        """Serialise to a dict suitable for ``table.put_item(Item=...)``.

        DynamoDB does not accept :class:`datetime` objects, so we convert
        them to ISO-8601 strings.  :class:`~.JobStatus` is a ``str`` enum
        so it serialises correctly without special handling.
        """
        item = {
            "job_id": self.job_id,
            "status": self.status.value,
            "filename": self.filename,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "s3_input_key": self.s3_input_key,
            "result_url": self.result_url,
            "error_message": self.error_message,
        }
        if self.s3_output_key is not None:
            item["s3_output_key"] = self.s3_output_key
        if self.expires_at is not None:
            item["expires_at"] = self.expires_at
        if self.worker_id is not None:
            item["worker_id"] = self.worker_id
        if self.lease_expires_at is not None:
            item["lease_expires_at"] = self.lease_expires_at
        if self.attempt_count:
            item["attempt_count"] = self.attempt_count
        if self.processing_version:
            item["processing_version"] = self.processing_version
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "DynamoDBJobItem":
        """Deserialise a raw DynamoDB ``get_item`` response dict.

        DynamoDB returns all string values as plain strings, so we parse
        ``created_at`` and ``updated_at`` back into :class:`datetime` objects
        and coerce ``status`` back into :class:`JobStatus`.
        """
        expires_val = item.get("expires_at")
        lease_val = item.get("lease_expires_at")
        return cls(
            job_id=item["job_id"],
            status=JobStatus(item["status"]),
            filename=item["filename"],
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            s3_input_key=item["s3_input_key"],
            s3_output_key=item.get("s3_output_key"),
            result_url=item.get("result_url"),
            error_message=item.get("error_message"),
            expires_at=int(expires_val) if expires_val is not None else None,
            worker_id=item.get("worker_id"),
            lease_expires_at=int(lease_val) if lease_val is not None else None,
            attempt_count=int(item.get("attempt_count", 0)),
            processing_version=int(item.get("processing_version", 0)),
        )
