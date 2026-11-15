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
from app.models.job import AcquireResult, DynamoDBJobItem, JobStatus
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
    # Processing Lease & Atomic Acquisition Operations
    # ------------------------------------------------------------------

    def acquire_job_lease(
        self,
        job_id: str,
        worker_id: str,
        lease_duration_seconds: int = 60,
        max_attempts: int = 3,
    ) -> tuple[AcquireResult, Optional[DynamoDBJobItem]]:
        """Atomically acquire or re-acquire a processing lease for a job.

        Acquisition succeeds if:
        1. status == 'PENDING'
        OR
        2. status == 'PROCESSING' and lease_expires_at < current_timestamp (previous worker crashed/timed out)
        AND
        3. attempt_count < max_attempts (retry threshold has not been exceeded)

        On success:
        - status becomes PROCESSING
        - worker_id is assigned
        - lease_expires_at is set to now + lease_duration_seconds
        - attempt_count is incremented
        - processing_version (fencing token) is incremented

        Returns
        -------
        tuple[AcquireResult, Optional[DynamoDBJobItem]]
            The acquisition result and the updated job item (if acquired or found).
        """
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        new_lease_expires = now_ts + lease_duration_seconds

        logger.info(
            "Attempting to acquire job lease",
            extra={"job_id": job_id, "worker_id": worker_id, "table": self.table_name},
        )

        condition = (
            Attr("job_id").exists()
            & (
                Attr("status").eq(JobStatus.PENDING.value)
                | (
                    Attr("status").eq(JobStatus.PROCESSING.value)
                    & Attr("lease_expires_at").lt(now_ts)
                )
            )
            & (
                Attr("attempt_count").not_exists()
                | Attr("attempt_count").lt(max_attempts)
            )
        )

        update_expr = (
            "SET #st = :processing, "
            "worker_id = :worker_id, "
            "lease_expires_at = :lease_expires, "
            "attempt_count = if_not_exists(attempt_count, :zero) + :one, "
            "processing_version = if_not_exists(processing_version, :zero) + :one, "
            "updated_at = :now_iso"
        )
        expr_names = {"#st": "status"}
        expr_values = {
            ":processing": JobStatus.PROCESSING.value,
            ":worker_id": worker_id,
            ":lease_expires": new_lease_expires,
            ":zero": 0,
            ":one": 1,
            ":now_iso": _now_iso(),
        }

        try:
            response = self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression=condition,
                ReturnValues="ALL_NEW",
            )
            updated_item = DynamoDBJobItem.from_dynamodb_item(response["Attributes"])
            logger.info(
                "Lease acquired successfully",
                extra={
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "version": updated_item.processing_version,
                    "attempt": updated_item.attempt_count,
                    "lease_expires_at": updated_item.lease_expires_at,
                },
            )
            return AcquireResult.ACQUIRED, updated_item

        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                # Inspect existing job state to diagnose why acquisition failed
                try:
                    current = self.get_job(job_id)
                except JobNotFoundError:
                    return AcquireResult.NOT_FOUND, None

                if current.status == JobStatus.COMPLETED:
                    logger.info("Job already completed — duplicate safe to acknowledge", extra={"job_id": job_id})
                    return AcquireResult.ALREADY_COMPLETED, current

                if current.attempt_count >= max_attempts:
                    logger.warning(
                        "Job exceeded maximum processing attempts — failing job",
                        extra={"job_id": job_id, "attempts": current.attempt_count, "max_attempts": max_attempts},
                    )
                    try:
                        self.update_job_status(
                            job_id=job_id,
                            status=JobStatus.FAILED,
                            error_message=f"Exceeded max processing attempts ({max_attempts})",
                        )
                    except Exception:
                        pass
                    return AcquireResult.MAX_ATTEMPTS_EXCEEDED, current

                if current.status == JobStatus.PROCESSING:
                    if current.lease_expires_at is not None and current.lease_expires_at >= now_ts:
                        logger.info(
                            "Job actively owned by another worker with valid lease",
                            extra={
                                "job_id": job_id,
                                "active_worker_id": current.worker_id,
                                "lease_expires_at": current.lease_expires_at,
                            },
                        )
                        return AcquireResult.ALREADY_ACTIVE, current

                if current.status == JobStatus.FAILED:
                    return AcquireResult.MAX_ATTEMPTS_EXCEEDED, current

            logger.error("Failed to acquire job lease in DynamoDB", extra={"job_id": job_id, "error": str(exc)})
            raise StorageError(f"Failed to acquire lease for job {job_id}", details={"job_id": job_id, "error": str(exc)}) from exc

    def renew_job_lease(
        self,
        job_id: str,
        worker_id: str,
        processing_version: int,
        extension_seconds: int = 60,
    ) -> bool:
        """Extend the lease timestamp for the active worker.

        Fails if another worker has already acquired a new lease generation (fencing).
        """
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        new_lease = now_ts + extension_seconds

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET lease_expires_at = :exp, updated_at = :now_iso",
                ExpressionAttributeValues={
                    ":exp": new_lease,
                    ":now_iso": _now_iso(),
                },
                ConditionExpression=(
                    Attr("job_id").exists()
                    & Attr("status").eq(JobStatus.PROCESSING.value)
                    & Attr("worker_id").eq(worker_id)
                    & Attr("processing_version").eq(processing_version)
                ),
            )
            logger.debug(
                "Lease renewed successfully",
                extra={"job_id": job_id, "worker_id": worker_id, "version": processing_version, "expires_at": new_lease},
            )
            return True
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Lease renewal rejected — worker has been fenced out by newer generation",
                    extra={"job_id": job_id, "worker_id": worker_id, "version": processing_version},
                )
                return False
            logger.error("Error renewing lease", extra={"job_id": job_id, "error": str(exc)})
            raise StorageError(f"Failed to renew lease for job {job_id}", details={"job_id": job_id, "error": str(exc)}) from exc

    def complete_job_with_lease(
        self,
        job_id: str,
        worker_id: str,
        processing_version: int,
        s3_output_key: str,
        result_url: Optional[str] = None,
    ) -> bool:
        """Mark job COMPLETED guarded by worker lease ownership and version fencing token."""
        set_parts = [
            "#st = :completed",
            "s3_output_key = :s3_output_key",
            "updated_at = :now_iso",
        ]
        expr_names = {"#st": "status"}
        expr_values: dict[str, Any] = {
            ":completed": JobStatus.COMPLETED.value,
            ":s3_output_key": s3_output_key,
            ":now_iso": _now_iso(),
        }
        if result_url is not None:
            set_parts.append("result_url = :result_url")
            expr_values[":result_url"] = result_url

        update_expr = "SET " + ", ".join(set_parts) + " REMOVE lease_expires_at"

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression=(
                    Attr("job_id").exists()
                    & Attr("status").eq(JobStatus.PROCESSING.value)
                    & Attr("worker_id").eq(worker_id)
                    & Attr("processing_version").eq(processing_version)
                ),
            )
            logger.info("Job marked COMPLETED under valid lease", extra={"job_id": job_id, "worker_id": worker_id, "version": processing_version})
            return True
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Completion rejected — worker fenced out by newer generation",
                    extra={"job_id": job_id, "worker_id": worker_id, "version": processing_version},
                )
                return False
            raise StorageError(f"Failed to complete job {job_id}", details={"job_id": job_id, "error": str(exc)}) from exc

    def fail_job_with_lease(
        self,
        job_id: str,
        worker_id: str,
        processing_version: int,
        error_message: str,
    ) -> bool:
        """Mark job FAILED guarded by worker lease ownership and version fencing token."""
        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #st = :failed, error_message = :err, updated_at = :now_iso REMOVE lease_expires_at",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={
                    ":failed": JobStatus.FAILED.value,
                    ":err": error_message,
                    ":now_iso": _now_iso(),
                },
                ConditionExpression=(
                    Attr("job_id").exists()
                    & Attr("status").eq(JobStatus.PROCESSING.value)
                    & Attr("worker_id").eq(worker_id)
                    & Attr("processing_version").eq(processing_version)
                ),
            )
            logger.info("Job marked FAILED under lease", extra={"job_id": job_id, "worker_id": worker_id})
            return True
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Fail update rejected — worker fenced out",
                    extra={"job_id": job_id, "worker_id": worker_id, "version": processing_version},
                )
                return False
            raise StorageError(f"Failed to update job {job_id} to FAILED", details={"job_id": job_id, "error": str(exc)}) from exc

    def claim_job(self, job_id: str, worker_id: str = "default-worker") -> bool:
        """Backward-compatible atomic job claim helper."""
        res, _ = self.acquire_job_lease(job_id=job_id, worker_id=worker_id)
        return res == AcquireResult.ACQUIRED

    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        result_url: Optional[str] = None,
        s3_output_key: Optional[str] = None,
        error_message: Optional[str] = None,
        expected_status: Optional[JobStatus] = None,
    ) -> None:
        """Partially update a job's status with optional optimistic concurrency.

        Parameters
        ----------
        job_id:
            Job to update.
        status:
            New ``JobStatus`` value.
        result_url:
            Optional presigned / public URL to the processing result.
        s3_output_key:
            Optional S3 key where the output artifact is permanently stored.
        error_message:
            Optional human-readable error message (populated on failure).
        expected_status:
            Optional guard ensuring the update only succeeds if the job is
            currently in this state (e.g. PROCESSING when setting COMPLETED).

        Raises
        ------
        JobNotFoundError
            If the job doesn't exist.
        StorageError
            On conditional check failure or any other DynamoDB / network failure.
        """
        logger.info(
            "Updating job status in DynamoDB",
            extra={
                "job_id": job_id,
                "new_status": status.value,
                "expected_status": expected_status.value if expected_status else None,
                "has_result_url": result_url is not None,
                "has_output_key": s3_output_key is not None,
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

        if s3_output_key is not None:
            set_parts.append("s3_output_key = :s3_output_key")
            expr_values[":s3_output_key"] = s3_output_key

        if error_message is not None:
            set_parts.append("error_message = :error_message")
            expr_values[":error_message"] = error_message

        update_expression = "SET " + ", ".join(set_parts)

        condition = Attr("job_id").exists()
        if expected_status is not None:
            condition = condition & Attr("status").eq(expected_status.value)

        try:
            self._table.update_item(
                Key={"job_id": job_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ConditionExpression=condition,
            )
        except _ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")  # type: ignore[attr-defined]
            if code == "ConditionalCheckFailedException":
                logger.warning(
                    "Cannot update status — conditional check failed (job missing or unexpected status)",
                    extra={"job_id": job_id, "expected_status": expected_status.value if expected_status else None},
                )
                # Check if job exists
                try:
                    self.get_job(job_id)
                    raise StorageError(
                        f"Conditional check failed updating job {job_id}: status was not {expected_status}",
                        details={"job_id": job_id, "expected_status": str(expected_status)},
                    ) from exc
                except JobNotFoundError:
                    raise JobNotFoundError(f"Job {job_id} not found", details={"job_id": job_id}) from exc
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
