"""Application-specific exception hierarchy.

Raise these inside service and worker layers; catch them in the API layer
to return appropriate HTTP responses.

Example::

    from app.core.exceptions import JobNotFoundError

    raise JobNotFoundError("job-123")
"""

from __future__ import annotations

from typing import Any, Optional


class AppError(Exception):
    """Base class for all application errors.

    Attributes:
        message: Human-readable description of what went wrong.
        details: Optional structured context (e.g. original exception, keys).
    """

    def __init__(
        self,
        message: str,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details: dict[str, Any] = details or {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(message={self.message!r}, details={self.details!r})"


class JobNotFoundError(AppError):
    """Raised when a requested job does not exist in DynamoDB."""

    def __init__(self, job_id: str, details: Optional[dict[str, Any]] = None) -> None:
        super().__init__(
            message=f"Job '{job_id}' not found.",
            details=details or {"job_id": job_id},
        )


class JobAlreadyExistsError(AppError):
    """Raised when attempting to create a job that already exists."""

    def __init__(self, job_id: str, details: Optional[dict[str, Any]] = None) -> None:
        super().__init__(
            message=f"Job '{job_id}' already exists.",
            details=details or {"job_id": job_id},
        )


class StorageError(AppError):
    """Raised when an S3 operation fails."""


class QueueError(AppError):
    """Raised when an SQS operation fails."""
