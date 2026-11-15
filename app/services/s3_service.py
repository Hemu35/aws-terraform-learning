"""
S3 Service
==========
Wraps all Amazon S3 operations used by the document processing API.

Key design decisions:
- All paths follow the convention ``{job_id}/input/{filename}`` (uploads) and
  ``{job_id}/output/result.json`` (results) so every job's assets are grouped
  under a single key prefix.
- Errors from boto3 are caught, logged with context, and re-raised as
  ``StorageError`` so callers never have to import botocore exceptions.
- Structured logging is used throughout; the logger name reflects the module
  path for easy filtering.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from app.core.exceptions import StorageError
from app.core.logging_config import get_logger
from app.services.aws_client import get_boto3_client

logger: logging.Logger = get_logger(__name__)


class S3Service:
    """High-level S3 operations for the document processor."""

    def __init__(self, settings: Any) -> None:
        self._client = get_boto3_client("s3", settings)
        self.bucket_name: str = settings.s3_bucket_name

    # ------------------------------------------------------------------
    # Upload helpers
    # ------------------------------------------------------------------

    def upload_document(
        self,
        job_id: str,
        filename: str,
        content: bytes,
        content_type: str,
    ) -> str:
        """Upload a raw document to S3 and return its key.

        Parameters
        ----------
        job_id:
            Unique job identifier — used as the key prefix.
        filename:
            Original filename; appended to the key.
        content:
            Raw file bytes.
        content_type:
            MIME type for the S3 object (e.g. ``"application/pdf"``).

        Returns
        -------
        str
            The S3 key the object was stored under.

        Raises
        ------
        StorageError
            On any S3 / network failure.
        """
        s3_key = f"{job_id}/input/{filename}"
        logger.info(
            "Uploading document to S3",
            extra={
                "job_id": job_id,
                "s3_key": s3_key,
                "bucket": self.bucket_name,
                "content_type": content_type,
                "size_bytes": len(content),
            },
        )
        try:
            self._client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type,
            )
        except Exception as exc:
            logger.error(
                "Failed to upload document to S3",
                extra={"job_id": job_id, "s3_key": s3_key, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to upload document for job {job_id}",
                details={"s3_key": s3_key, "error": str(exc)},
            ) from exc

        logger.info(
            "Document uploaded successfully",
            extra={"job_id": job_id, "s3_key": s3_key},
        )
        return s3_key

    def upload_result(self, job_id: str, result: dict, version: Optional[int] = None) -> str:
        """Persist a processing result dict as JSON to S3.

        Parameters
        ----------
        job_id:
            Job identifier — used as the key prefix.
        result:
            Arbitrary dict to serialise as JSON.
        version:
            Optional processing version token. When supplied, stores output at
            a versioned key (`result_v{version}.json`) to guarantee idempotency
            and prevent race condition overwrites between worker attempts.

        Returns
        -------
        str
            The S3 key the result was stored under.

        Raises
        ------
        StorageError
            On any S3 / network failure.
        """
        if version is not None:
            s3_key = f"{job_id}/output/result_v{version}.json"
        else:
            s3_key = f"{job_id}/output/result.json"
        payload = json.dumps(result, default=str).encode()
        logger.info(
            "Uploading result to S3",
            extra={"job_id": job_id, "s3_key": s3_key, "bucket": self.bucket_name},
        )
        try:
            self._client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=payload,
                ContentType="application/json",
            )
        except Exception as exc:
            logger.error(
                "Failed to upload result to S3",
                extra={"job_id": job_id, "s3_key": s3_key, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to upload result for job {job_id}",
                details={"s3_key": s3_key, "error": str(exc)},
            ) from exc

        logger.info(
            "Result uploaded successfully",
            extra={"job_id": job_id, "s3_key": s3_key},
        )
        return s3_key

    # ------------------------------------------------------------------
    # Download helpers
    # ------------------------------------------------------------------

    def get_result(self, job_id: str, s3_key: Optional[str] = None) -> Optional[bytes]:
        """Download the processing result for a job, if it exists.

        Parameters
        ----------
        job_id:
            Job identifier.
        s3_key:
            Optional specific S3 key to retrieve (e.g. versioned key). Defaults
            to `{job_id}/output/result.json`.

        Returns
        -------
        bytes or None
            Raw bytes of the result JSON, or ``None`` if the object does
            not yet exist (the job may still be processing).

        Raises
        ------
        StorageError
            On unexpected S3 failures (not a simple "not found").
        """
        target_key = s3_key or f"{job_id}/output/result.json"
        logger.info(
            "Fetching result from S3",
            extra={"job_id": job_id, "s3_key": target_key},
        )
        try:
            response = self._client.get_object(Bucket=self.bucket_name, Key=target_key)
            return response["Body"].read()
        except self._client.exceptions.NoSuchKey:
            logger.info(
                "Result not found in S3 (job may still be processing)",
                extra={"job_id": job_id, "s3_key": target_key},
            )
            return None
        except Exception as exc:
            # Handle botocore ClientError for NoSuchKey when exceptions attr
            # is not present (e.g. in some mocked environments).
            error_code = getattr(
                getattr(exc, "response", {}).get("Error", {}), "get", lambda k, d=None: d
            )("Code")
            if error_code == "NoSuchKey":
                return None
            logger.error(
                "Failed to fetch result from S3",
                extra={"job_id": job_id, "s3_key": s3_key, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to retrieve result for job {job_id}",
                details={"s3_key": s3_key, "error": str(exc)},
            ) from exc

    # ------------------------------------------------------------------
    # Presigned URLs
    # ------------------------------------------------------------------

    def generate_presigned_url(self, s3_key: str, expiry: int = 3600) -> str:
        """Generate a presigned GET URL for an S3 object.

        Parameters
        ----------
        s3_key:
            Full S3 key of the object.
        expiry:
            URL lifetime in seconds (default 1 hour).

        Returns
        -------
        str
            Presigned URL string.

        Raises
        ------
        StorageError
            On failure to generate the URL.
        """
        logger.info(
            "Generating presigned URL",
            extra={"s3_key": s3_key, "expiry_seconds": expiry},
        )
        try:
            url: str = self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": s3_key},
                ExpiresIn=expiry,
            )
        except Exception as exc:
            logger.error(
                "Failed to generate presigned URL",
                extra={"s3_key": s3_key, "error": str(exc)},
            )
            raise StorageError(
                "Failed to generate presigned URL",
                details={"s3_key": s3_key, "error": str(exc)},
            ) from exc

        return url
