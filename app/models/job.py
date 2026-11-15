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

    Extends :class:`JobResponse` with the S3 key of the original upload so
    that the worker can retrieve the file without re-deriving the key.
    """

    job_id: str
    status: JobStatus
    filename: str
    created_at: datetime
    updated_at: datetime
    s3_input_key: str
    result_url: Optional[str] = None
    error_message: Optional[str] = None

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def to_dynamodb_item(self) -> dict:
        """Serialise to a dict suitable for ``table.put_item(Item=...)``.

        DynamoDB does not accept :class:`datetime` objects, so we convert
        them to ISO-8601 strings.  :class:`~.JobStatus` is a ``str`` enum
        so it serialises correctly without special handling.
        """
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "filename": self.filename,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "s3_input_key": self.s3_input_key,
            "result_url": self.result_url,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dynamodb_item(cls, item: dict) -> "DynamoDBJobItem":
        """Deserialise a raw DynamoDB ``get_item`` response dict.

        DynamoDB returns all string values as plain strings, so we parse
        ``created_at`` and ``updated_at`` back into :class:`datetime` objects
        and coerce ``status`` back into :class:`JobStatus`.
        """
        return cls(
            job_id=item["job_id"],
            status=JobStatus(item["status"]),
            filename=item["filename"],
            created_at=datetime.fromisoformat(item["created_at"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
            s3_input_key=item["s3_input_key"],
            result_url=item.get("result_url"),
            error_message=item.get("error_message"),
        )
