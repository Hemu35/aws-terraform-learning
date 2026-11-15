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
from datetime import datetime, timezone
from typing import Any

from app.core.exceptions import StorageError
from app.core.logging_config import get_logger
from app.models.job import JobStatus
from app.services.dynamodb_service import DynamoDBService
from app.services.s3_service import S3Service
from app.services.sqs_service import SQSService

logger: logging.Logger = get_logger(__name__)


class JobProcessor:
    """Consumes SQS messages and processes documents end-to-end."""

    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._s3 = S3Service(settings)
        self._sqs = SQSService(settings)
        self._dynamo = DynamoDBService(settings)

    # ------------------------------------------------------------------
    # Core processing logic
    # ------------------------------------------------------------------

    def _download_document(self, s3_key: str) -> bytes:
        """Download a raw document from S3 by its full key.

        This is the worker counterpart to ``S3Service.upload_document``.
        Unlike ``get_result`` (which assumes the output prefix), this method
        accepts any key — typically ``{job_id}/input/{filename}``.
        """
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

    def process_job(self, job_id: str) -> None:
        """Process a single job end-to-end.

        Steps
        -----
        1. Mark job PROCESSING in DynamoDB.
        2. Retrieve s3_input_key from DynamoDB.
        3. Download the raw document from S3.
        4. Simulate processing (word/char count + metadata).
        5. Upload result JSON to S3.
        6. Generate presigned URL for the result.
        7. Mark job COMPLETED with result_url.

        On any failure the job is marked FAILED and the exception is re-raised
        so the caller can decide whether to delete the SQS message.
        """
        logger.info("Starting processing for job", extra={"job_id": job_id})

        try:
            # 1. Flip status → PROCESSING
            self._dynamo.update_job_status(job_id=job_id, status=JobStatus.PROCESSING)

            # 2. Load the job item to find the S3 input key.
            job_item = self._dynamo.get_job(job_id)
            s3_input_key = job_item.s3_input_key
            logger.info(
                "Retrieved job item",
                extra={"job_id": job_id, "s3_input_key": s3_input_key},
            )

            # 3. Download the document.
            content = self._download_document(s3_input_key)

            # 4. Simulate processing.
            result = self._simulate_processing(job_id, s3_input_key, content)
            logger.info(
                "Processing complete",
                extra={"job_id": job_id, "word_count": result["word_count"]},
            )

            # 5. Upload result JSON to S3.
            result_key = self._s3.upload_result(job_id, result)

            # 6. Generate presigned URL (1 hour TTL).
            presigned_url = self._s3.generate_presigned_url(result_key)

            # 7. Mark COMPLETED.
            self._dynamo.update_job_status(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                result_url=presigned_url,
            )
            logger.info("Job completed successfully", extra={"job_id": job_id})

        except Exception as exc:
            logger.error(
                "Job processing failed — marking FAILED",
                extra={"job_id": job_id, "error": str(exc)},
            )
            try:
                self._dynamo.update_job_status(
                    job_id=job_id,
                    status=JobStatus.FAILED,
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
        - Each message is processed; ``delete_message`` is called only on
          success *or* graceful FAILED transition (to avoid infinite requeueing
          of unprocessable messages).
        - ``KeyboardInterrupt`` is caught for graceful Ctrl-C shutdown.
        """
        logger.info("Worker starting — polling SQS for jobs")
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
                        self.process_job(job_id)
                        logger.info("Job processed successfully", extra={"job_id": job_id})

                    except Exception as exc:
                        # process_job already marked the job FAILED; delete the
                        # message so it doesn't requeue and block the queue.
                        logger.error(
                            "Error processing job — deleting message to avoid poison-pill requeue",
                            extra={"job_id": job_id, "error": str(exc)},
                        )
                    finally:
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
