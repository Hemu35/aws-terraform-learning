"""
Background Job Processor
=========================
Runs as a separate process, polling SQS for job messages, processing each
document, and writing results back to S3 and DynamoDB.

Design decisions:
- ``JobProcessor`` owns its own boto3 clients (via service layer) so the worker
  process is fully self-contained with no shared state across OS processes.
- Processing is simulated (word/char count + metadata extraction) to keep the
  demo runnable without real ML dependencies; swap ``_simulate_processing`` for
  a real call when ready.
- Exceptions inside ``process_job`` always flip the job to FAILED so DynamoDB
  stays consistent and the caller (``run``) can still delete the SQS message,
  avoiding infinite requeue loops for poison messages.
- The SQS message is deleted *after* processing, not before, so a crash before
  completion causes the message to become visible again after the visibility
  timeout and the job is retried (at-least-once delivery).
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from app.core.exceptions import StorageError
from app.core.logging_config import get_logger
from app.models.job import AcquireResult, JobStatus
from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService

logger: logging.Logger = get_logger(__name__)


class VisibilityHeartbeat:
    """Background heartbeat that periodically extends SQS message visibility timeout
    and renews the DynamoDB processing lease.

    Runs in a background thread to prevent duplicate delivery and lease expiration
    when document processing takes longer than default timeouts.
    If lease renewal is rejected (fenced out by a newer worker generation),
    the thread signals `is_fenced_out` so the processor can abort safely.
    """

    def __init__(
        self,
        sqs: SQSService,
        receipt_handle: str,
        interval_seconds: int = 20,
        extension_seconds: int = 60,
        dynamo: Optional[DynamoDBService] = None,
        job_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        processing_version: Optional[int] = None,
    ) -> None:
        self._sqs = sqs
        self._receipt_handle = receipt_handle
        self._interval = interval_seconds
        self._extension = extension_seconds
        self._dynamo = dynamo
        self._job_id = job_id
        self._worker_id = worker_id
        self._processing_version = processing_version
        self._stop_event = threading.Event()
        self._fenced_out_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_fenced_out(self) -> bool:
        """Returns True if this worker was fenced out during lease renewal."""
        return self._fenced_out_event.is_set()

    def __enter__(self) -> "VisibilityHeartbeat":
        if self._receipt_handle or (self._dynamo and self._job_id):
            self._thread = threading.Thread(target=self._run, daemon=True, name="worker-heartbeat")
            self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            # 1. Renew DynamoDB lease if configured
            if self._dynamo and self._job_id and self._worker_id and self._processing_version:
                try:
                    renewed = self._dynamo.renew_job_lease(
                        job_id=self._job_id,
                        worker_id=self._worker_id,
                        processing_version=self._processing_version,
                        extension_seconds=self._extension,
                    )
                    if not renewed:
                        logger.warning(
                            "DynamoDB lease renewal rejected — worker fenced out by newer generation",
                            extra={
                                "job_id": self._job_id,
                                "worker_id": self._worker_id,
                                "version": self._processing_version,
                            },
                        )
                        self._fenced_out_event.set()
                        return
                except Exception as exc:
                    logger.warning(
                        "Heartbeat failed to renew DynamoDB lease",
                        extra={"job_id": self._job_id, "error": str(exc)},
                    )

            # 2. Extend SQS visibility timeout
            if self._receipt_handle:
                try:
                    self._sqs.change_message_visibility(
                        self._receipt_handle,
                        self._extension,
                    )
                    logger.debug(
                        "Visibility heartbeat extended message visibility",
                        extra={"extension_seconds": self._extension},
                    )
                except Exception as exc:
                    logger.warning(
                        "Heartbeat failed to extend SQS message visibility",
                        extra={"error": str(exc)},
                    )


class JobProcessor:
    """Consumes SQS messages and processes documents end-to-end with distributed leases."""

    def __init__(self, settings: Any, worker_id: Optional[str] = None) -> None:
        self._settings = settings
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._s3 = S3Service(settings)
        self._sqs = SQSService(settings)
        self._dynamo = DynamoDBService(settings)

    # ------------------------------------------------------------------
    # Core processing logic
    # ------------------------------------------------------------------

    def _download_document(self, s3_key: str) -> bytes:
        """Download a raw document from S3 by its full key."""
        logger.info("Downloading document from S3", extra={"s3_key": s3_key})
        try:
            response = self._s3._client.get_object(
                Bucket=self._s3.bucket_name,
                Key=s3_key,
            )
            data: bytes = response["Body"].read()
            logger.info(
                "Document downloaded",
                extra={"s3_key": s3_key, "size_bytes": len(data)},
            )
            return data
        except Exception as exc:
            logger.error(
                "Failed to download document",
                extra={"s3_key": s3_key, "error": str(exc)},
            )
            raise StorageError(
                f"Failed to download document at {s3_key}",
                details={"s3_key": s3_key, "error": str(exc)},
            ) from exc

    @staticmethod
    def _simulate_processing(job_id: str, s3_key: str, content: bytes) -> dict:
        """Simulate document analysis — word count, char count, metadata."""
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception:
            text = ""

        words = text.split()
        filename = s3_key.split("/")[-1] if "/" in s3_key else s3_key

        return {
            "job_id": job_id,
            "filename": filename,
            "size_bytes": len(content),
            "char_count": len(text),
            "word_count": len(words),
            "line_count": text.count("\n") + 1 if text else 0,
            "processed_at": datetime.now(tz=timezone.utc).isoformat(),
            "summary": (
                f"Document '{filename}' contains {len(words)} words "
                f"across {text.count(chr(10)) + 1} lines."
            ),
        }

    def process_job(self, job_id: str, receipt_handle: str = "") -> bool:
        """Process a single job end-to-end with atomic lease acquisition, version fencing, and heartbeat.

        Returns
        -------
        bool
            True if the SQS message should be deleted (job completed, duplicate, or terminal failure).
            False if the SQS message should NOT be deleted (job actively owned by another healthy worker,
            or worker was fenced out).
        """
        logger.info(
            "Starting processing attempt for job",
            extra={"job_id": job_id, "worker_id": self.worker_id},
        )

        lease_duration = getattr(self._settings, "lease_duration_seconds", 60)
        max_attempts = getattr(self._settings, "max_job_attempts", 3)
        renew_interval = getattr(self._settings, "lease_renew_interval_seconds", 20)

        # 1. Acquire distributed lease with fencing token (processing_version)
        acquire_result, job_item = self._dynamo.acquire_job_lease(
            job_id=job_id,
            worker_id=self.worker_id,
            lease_duration_seconds=lease_duration,
            max_attempts=max_attempts,
        )

        if acquire_result == AcquireResult.ALREADY_COMPLETED:
            logger.info(
                "Job already completed — acknowledging duplicate SQS message",
                extra={"job_id": job_id},
            )
            return True

        if acquire_result == AcquireResult.ALREADY_ACTIVE:
            logger.info(
                "Job actively owned by another worker with valid lease — retaining message in SQS for future recovery",
                extra={"job_id": job_id},
            )
            return False

        if acquire_result == AcquireResult.MAX_ATTEMPTS_EXCEEDED:
            logger.warning(
                "Job exceeded max processing attempts — acknowledging message to avoid poison-pill queue block",
                extra={"job_id": job_id},
            )
            return True

        if acquire_result == AcquireResult.NOT_FOUND or job_item is None:
            logger.error("Job not found in DynamoDB — dropping message", extra={"job_id": job_id})
            return True

        # At this point, acquire_result == AcquireResult.ACQUIRED
        version = job_item.processing_version or 1
        logger.info(
            "Acquired lease for job",
            extra={
                "job_id": job_id,
                "worker_id": self.worker_id,
                "version": version,
                "attempt": job_item.attempt_count,
            },
        )

        # Run processing under a dual heartbeat: SQS visibility extension + DynamoDB lease renewal
        with VisibilityHeartbeat(
            sqs=self._sqs,
            receipt_handle=receipt_handle,
            interval_seconds=renew_interval,
            extension_seconds=lease_duration,
            dynamo=self._dynamo,
            job_id=job_id,
            worker_id=self.worker_id,
            processing_version=version,
        ) as heartbeat:
            try:
                # 2. Get input key from job item
                s3_input_key = job_item.s3_input_key
                logger.info(
                    "Retrieved job input key",
                    extra={"job_id": job_id, "s3_input_key": s3_input_key},
                )

                # 3. Download the document
                content = self._download_document(s3_input_key)

                # Check fencing before expensive simulation
                if heartbeat.is_fenced_out:
                    logger.warning(
                        "Worker was fenced out during download — aborting without committing",
                        extra={"job_id": job_id, "worker_id": self.worker_id, "version": version},
                    )
                    return False

                # 4. Simulate processing
                result = self._simulate_processing(job_id, s3_input_key, content)
                logger.info(
                    "Processing complete",
                    extra={"job_id": job_id, "word_count": result["word_count"]},
                )

                # Check fencing before writing to S3
                if heartbeat.is_fenced_out:
                    logger.warning(
                        "Worker was fenced out during processing — aborting S3 result write",
                        extra={"job_id": job_id, "worker_id": self.worker_id, "version": version},
                    )
                    return False

                # 5. Upload result JSON to S3 with versioned output key for idempotency
                result_key = self._s3.upload_result(job_id, result, version=version)

                # 6. Generate presigned URL for the result
                presigned_url = self._s3.generate_presigned_url(result_key)

                # 7. Complete job in DynamoDB guarded by lease ownership and fencing token
                completed = self._dynamo.complete_job_with_lease(
                    job_id=job_id,
                    worker_id=self.worker_id,
                    processing_version=version,
                    s3_output_key=result_key,
                    result_url=presigned_url,
                )

                if not completed:
                    logger.warning(
                        "Completion rejected by DynamoDB conditional check — worker was fenced out by a newer worker",
                        extra={"job_id": job_id, "worker_id": self.worker_id, "version": version},
                    )
                    return False

                logger.info(
                    "Job completed successfully under lease",
                    extra={"job_id": job_id, "version": version},
                )
                return True

            except Exception as exc:
                logger.error(
                    "Job processing failed with exception — attempting to fail with lease",
                    extra={"job_id": job_id, "error": str(exc)},
                )
                try:
                    self._dynamo.fail_job_with_lease(
                        job_id=job_id,
                        worker_id=self.worker_id,
                        processing_version=version,
                        error_message=str(exc),
                    )
                except Exception as dynamo_exc:
                    logger.error(
                        "Could not update job to FAILED in DynamoDB",
                        extra={"job_id": job_id, "error": str(dynamo_exc)},
                    )
                raise

    # ------------------------------------------------------------------
    # Main polling loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Poll SQS indefinitely, processing one message at a time.

        SQS message lifecycle
        ---------------------
        - ``receive_messages`` returns list of dicts with ``job_id`` and ``receipt_handle``.
        - Each message is processed via ``process_job``:
          - Returns ``True``: message was processed, completed, or terminally failed -> delete message.
          - Returns ``False``: job is actively owned by another healthy worker or worker was fenced out -> DO NOT delete message.
        - Unhandled exceptions delete the message to avoid poison-pill queue block.
        - ``KeyboardInterrupt`` is caught for graceful Ctrl-C shutdown.
        """
        logger.info(
            "Worker starting — polling SQS for jobs",
            extra={"worker_id": self.worker_id},
        )
        try:
            while True:
                messages = self._sqs.receive_messages()
                if not messages:
                    logger.debug("No messages received — continuing poll")
                    continue

                for message in messages:
                    receipt_handle = message.get("receipt_handle", "")
                    job_id = message.get("job_id", "")

                    if not job_id:
                        logger.warning("Message missing job_id — skipping", extra={"message": message})
                        if receipt_handle:
                            self._sqs.delete_message(receipt_handle)
                        continue

                    try:
                        logger.info(
                            "Processing SQS job",
                            extra={"job_id": job_id, "receipt_handle": receipt_handle[:20]},
                        )
                        should_delete = self.process_job(job_id, receipt_handle=receipt_handle)
                        if should_delete and receipt_handle:
                            self._sqs.delete_message(receipt_handle)
                            logger.info("Job processed and SQS message acknowledged", extra={"job_id": job_id})
                        elif not should_delete:
                            logger.info(
                                "Retaining SQS message in queue for recovery or active worker",
                                extra={"job_id": job_id},
                            )

                    except Exception as exc:
                        # process_job attempted to mark the job FAILED; delete the
                        # message so it doesn't requeue and block the queue.
                        logger.error(
                            "Error processing job — deleting message to avoid poison-pill requeue",
                            extra={"job_id": job_id, "error": str(exc)},
                        )
                        if receipt_handle:
                            try:
                                self._sqs.delete_message(receipt_handle)
                            except Exception as del_exc:
                                logger.warning(
                                    "Failed to delete SQS message",
                                    extra={
                                        "job_id": job_id,
                                        "receipt_handle": receipt_handle[:20],
                                        "error": str(del_exc),
                                    },
                                )

        except KeyboardInterrupt:
            logger.info("Worker received KeyboardInterrupt — shutting down gracefully")
