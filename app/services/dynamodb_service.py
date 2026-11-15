"""
DynamoDB Service
================
Wraps all Amazon DynamoDB operations for the jobs table.

Design decisions:
- Uses ``boto3.resource`` (the higher-level ORM-like API) instead of the
  low-level ``boto3.client`` so item gets / puts use Python dicts directly.
- ``create_job`` uses a ``ConditionExpression`` (attribute_not_exists) to
  prevent silently overwriting an existing job — a double-submit will raise
  ``JobAlreadyExistsError`` instead of corrupting state.
- ``update_job_status`` uses ``UpdateExpression`` rather than ``PutItem`` so
  only the specified attributes are mutated; the full job record is preserved.
- All boto3 / botocore errors are caught and re-raised as domain exceptions
  defined in ``app.core.exceptions``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from boto3.dynamodb.conditions import Attr

from app.core.exceptions import JobAlreadyExistsError, JobNotFoundError, StorageError
from app.core.logging_config import get_logger
from app.models.job import DynamoDBJobItem, JobStatus
from app.services.aws_client import get_boto3_resource

logger: logging.Logger = get_logger(__name__)

# botocore is an indirect dependency of boto3; import lazily to avoid issues
# in environments where only boto3 stubs are installed.
try:
    from botocore.exceptions import ClientError as _ClientError
except ImportError:  # pragma: no cover
    _ClientError = Exception  # type: ignore[assignment,misc]


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


class DynamoDBService:
    """High-level DynamoDB operations for the document processor jobs table."""

    def __init__(self, settings: Any) -> None:
        self._resource = get_boto3_resource("dynamodb", settings)
        self.table_name: str = settings.dynamodb_table_name
        self._table = self._resource.Table(self.table_name)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def create_job(self, item: DynamoDBJobItem) -> None:
        """Persist a new job record to DynamoDB.

        Uses a ``ConditionExpression`` to guarantee idempotency: if a job
        with the same ``job_id`` already exists, ``JobAlreadyExistsError``
        is raised instead of silently overwriting it.

        Parameters
        ----------
        item:
            Fully populated ``DynamoDBJobItem`` instance.

        Raises
        ------
        JobAlreadyExistsError
            If a job with the same ``job_id`` is already in the table.
        StorageError
            On any other DynamoDB / network failure.
        """
        dynamo_item = item.to_dynamodb_item()
        logger.info(
            "Creating job in DynamoDB",
            extra={"job_id": item.job_id, "table": self.table_name},
        )
        try:
            self._table.put_item(
                Item=dynamo_item,
                ConditionExpression=Attr("job_id").not_exists(),
            )
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")  # type: ignore[attr-defined]
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Job already exists in DynamoDB",
                    extra={"job_id": item.job_id},
                )
                raise JobAlreadyExistsError(
                    f"Job {item.job_id} already exists",
                    details={"job_id": item.job_id},
                ) from exc
            logger.error(
                "Failed to create job in DynamoDB",
                extra={"job_id": item.job_id, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to create job {item.job_id}",
                details={"job_id": item.job_id, "error": str(exc)},
            ) from exc
        except Exception as exc:
            logger.error(
                "Unexpected error creating job in DynamoDB",
                extra={"job_id": item.job_id, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to create job {item.job_id}",
                details={"job_id": item.job_id, "error": str(exc)},
            ) from exc

        logger.info(
            "Job created successfully in DynamoDB",
            extra={"job_id": item.job_id},
        )

    def get_job(self, job_id: str) -> DynamoDBJobItem:
        """Retrieve a job record from DynamoDB.

        Parameters
        ----------
        job_id:
            Unique job identifier.

        Returns
        -------
        DynamoDBJobItem
            The deserialized job record.

        Raises
        ------
        JobNotFoundError
            If no item with the given ``job_id`` exists in the table.
        StorageError
            On any other DynamoDB / network failure.
        """
        logger.info(
            "Fetching job from DynamoDB",
            extra={"job_id": job_id, "table": self.table_name},
        )
        try:
            response = self._table.get_item(Key={"job_id": job_id})
        except Exception as exc:
            logger.error(
                "Failed to fetch job from DynamoDB",
                extra={"job_id": job_id, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to retrieve job {job_id}",
                details={"job_id": job_id, "error": str(exc)},
            ) from exc

        item = response.get("Item")
        if item is None:
            logger.warning(
                "Job not found in DynamoDB",
                extra={"job_id": job_id},
            )
            raise JobNotFoundError(
                f"Job {job_id} not found",
                details={"job_id": job_id},
            )

        logger.info("Job fetched successfully", extra={"job_id": job_id})
        return DynamoDBJobItem.from_dynamodb_item(item)

    # ------------------------------------------------------------------
    # Update operations
    # ------------------------------------------------------------------

    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result_url: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Partially update a job's status (and optional related fields).

        Uses ``UpdateExpression`` so only the listed attributes are written;
        the rest of the item (e.g. ``filename``, ``s3_input_key``) are left
        untouched.

        Parameters
        ----------
        job_id:
            Job to update.
        status:
            New ``JobStatus`` value.
        result_url:
            Optional presigned / public URL to the processing result.
        error_message:
            Optional human-readable error message (populated on failure).

        Raises
        ------
        JobNotFoundError
            If the job doesn't exist (via ``ConditionExpression``).
        StorageError
            On any other DynamoDB / network failure.
        """
        logger.info(
            "Updating job status in DynamoDB",
            extra={
                "job_id": job_id,
                "new_status": status.value,
                "has_result_url": result_url is not None,
                "has_error": error_message is not None,
            },
        )

        # Build UpdateExpression dynamically so we don't set null attributes.
        set_parts: list[str] = [
            "#st = :status",
            "updated_at = :updated_at",
        ]
        expr_names: dict[str, str] = {"#st": "status"}  # 'status' is a reserved word
        expr_values: dict[str, Any] = {
            ":status": status.value,
            ":updated_at": _now_iso(),
        }

        if result_url is not None:
            set_parts.append("result_url = :result_url")
            expr_values[":result_url"] = result_url

        if error_message is not None:
            set_parts.append("error_message = :error_message")
            expr_values[":error_message"] = error_message

        update_expression = "SET " + ", ".join(set_parts)

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression=Attr("job_id").exists(),
            )
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")  # type: ignore[attr-defined]
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Cannot update status — job not found",
                    extra={"job_id": job_id},
                )
                raise JobNotFoundError(
                    f"Job {job_id} not found",
                    details={"job_id": job_id},
                ) from exc
            logger.error(
                "Failed to update job status in DynamoDB",
                extra={"job_id": job_id, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to update job {job_id}",
                details={"job_id": job_id, "error": str(exc)},
            ) from exc
        except Exception as exc:
            logger.error(
                "Unexpected error updating job status",
                extra={"job_id": job_id, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to update job {job_id}",
                details={"job_id": job_id, "error": str(exc)},
            ) from exc

        logger.info(
            "Job status updated successfully",
            extra={"job_id": job_id, "status": status.value},
        )
