"""
Worker Entry Point
==================
Run this module directly to start the background job processor:

    python -m app.worker.main

The worker polls SQS indefinitely for job messages and processes each document
end-to-end (download from S3 → analyse → upload result → update DynamoDB).
"""

from __future__ import annotations

if __name__ == "__main__":
    from app.core.config import get_settings
    from app.core.logging_config import setup_logging
    from app.worker.processor import JobProcessor

    settings = get_settings()
    setup_logging(settings.log_level)

    processor = JobProcessor(settings)
    processor.run()
