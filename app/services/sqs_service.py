"""
SQS Service
===========
Wraps Amazon SQS send / receive / delete operations for the job queue.

Design notes:
- Supports both standard and FIFO queues transparently: the ``send_job``
  method checks whether the queue URL ends with ``.fifo`` and only sends
  ``MessageDeduplicationId`` / ``MessageGroupId`` for FIFO queues.
- Long polling (``WaitTimeSeconds``) is used in ``receive_messages`` to
  reduce empty responses and lower request costs.
- All boto3 errors are caught, logged, and re-raised as ``QueueError``.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.core.exceptions import QueueError
from app.core.logging_config import get_logger
from app.services.aws_client import get_boto3_client

logger: logging.Logger = get_logger(__name__)


class SQSService:
    """High-level SQS operations for the document processor job queue."""

    def __init__(self, settings: Any) -> None:
        self._client = get_boto3_client("sqs", settings)
        self.queue_url: str = settings.sqs_queue_url

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def _is_fifo(self) -> bool:
        """Return True if the configured queue is a FIFO queue."""
        return self.queue_url.endswith(".fifo")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_job(self, job_id: str) -> str:
        """Enqueue a job for asynchronous processing.

        Parameters
        ----------
        job_id:
            Unique identifier for the processing job.

        Returns
        -------
        str
            The SQS ``MessageId`` assigned by the service.

        Raises
        ------
        QueueError
            On any SQS / network failure.
        """
        body = json.dumps({"job_id": job_id})
        send_kwargs: dict[str, Any] = {
            "QueueUrl": self.queue_url,
            "MessageBody": body,
        }

        if self._is_fifo:
            # FIFO queues require deduplication and group IDs.
            send_kwargs["MessageDeduplicationId"] = job_id
            send_kwargs["MessageGroupId"] = "jobs"

        logger.info(
            "Sending job to SQS",
            extra={
                "job_id": job_id,
                "queue_url": self.queue_url,
                "is_fifo": self._is_fifo,
            },
        )
        try:
            response = self._client.send_message(**send_kwargs)
        except Exception as exc:
            logger.error(
                "Failed to send job to SQS",
                extra={"job_id": job_id, "error": str(exc)},
            )
            raise QueueError(
                f"Failed to enqueue job {job_id}",
                details={"job_id": job_id, "error": str(exc)},
            ) from exc

        message_id: str = response["MessageId"]
        logger.info(
            "Job enqueued successfully",
            extra={"job_id": job_id, "message_id": message_id},
        )
        return message_id

    def receive_messages(
        self,
        max_messages: int = 10,
        wait_seconds: int = 20,
    ) -> list[dict]:
        """Long-poll the queue and return normalised message dicts.

        Each returned dict has the keys:
        - ``job_id`` (str): parsed from the message body.
        - ``receipt_handle`` (str): required to delete the message after
          successful processing.

        Parameters
        ----------
        max_messages:
            Maximum number of messages to retrieve (1–10).
        wait_seconds:
            SQS long-poll duration in seconds (0–20).

        Returns
        -------
        list[dict]
            Possibly empty list of normalised message dicts.

        Raises
        ------
        QueueError
            On any SQS / network failure.
        """
        logger.info(
            "Polling SQS for messages",
            extra={
                "queue_url": self.queue_url,
                "max_messages": max_messages,
                "wait_seconds": wait_seconds,
            },
        )
        try:
            response = self._client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_seconds,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )
        except Exception as exc:
            logger.error(
                "Failed to receive messages from SQS",
                extra={"error": str(exc)},
            )
            raise QueueError(
                "Failed to receive messages from SQS",
                details={"error": str(exc)},
            ) from exc

        raw_messages = response.get("Messages", [])
        logger.info(
            "Received messages from SQS",
            extra={"count": len(raw_messages)},
        )

        parsed: list[dict] = []
        for msg in raw_messages:
            try:
                body = json.loads(msg["Body"])
                parsed.append(
                    {
                        "job_id": body["job_id"],
                        "receipt_handle": msg["ReceiptHandle"],
                    }
                )
            except (KeyError, json.JSONDecodeError) as exc:
                logger.warning(
                    "Skipping malformed SQS message",
                    extra={"message_id": msg.get("MessageId"), "error": str(exc)},
                )

        return parsed

    def change_message_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        """Extend the visibility timeout for an in-flight message (heartbeat).

        Parameters
        ----------
        receipt_handle:
            The receipt handle returned by ``receive_messages``.
        visibility_timeout:
            New visibility timeout in seconds (0 to 43200).

        Raises
        ------
        QueueError
            On any SQS / network failure.
        """
        logger.info(
            "Extending SQS message visibility timeout",
            extra={"queue_url": self.queue_url, "visibility_timeout": visibility_timeout},
        )
        try:
            self._client.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        except Exception as exc:
            logger.error(
                "Failed to change SQS message visibility",
                extra={"error": str(exc)},
            )
            raise QueueError(
                "Failed to change SQS message visibility",
                details={"error": str(exc)},
            ) from exc

    def delete_message(self, receipt_handle: str) -> None:
        """Remove a successfully processed message from the queue.

        Parameters
        ----------
        receipt_handle:
            The receipt handle returned by ``receive_messages``.

        Raises
        ------
        QueueError
            On any SQS / network failure.
        """
        logger.info(
            "Deleting message from SQS",
            extra={"queue_url": self.queue_url},
        )
        try:
            self._client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        except Exception as exc:
            logger.error(
                "Failed to delete SQS message",
                extra={"error": str(exc)},
            )
            raise QueueError(
                "Failed to delete SQS message",
                details={"error": str(exc)},
            ) from exc

        logger.info("SQS message deleted successfully")
