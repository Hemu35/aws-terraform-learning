from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file.

    For local development all AWS calls are routed to LocalStack via
    ``aws_endpoint_url``.  In production that field is left unset so boto3
    uses the real AWS endpoints.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # AWS credentials — never hardcode; always come from env
    aws_default_region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"

    # Optional: set to LocalStack URL for local dev; unset for real AWS
    aws_endpoint_url: Optional[str] = None

    # Resource names
    dynamodb_table_name: str = "jobs"
    s3_bucket_name: str = "documents"
    sqs_queue_url: str = "http://localhost:4566/000000000000/jobs-queue"

    # Observability
    log_level: str = "INFO"

    # Runtime environment
    environment: str = "local"

    # Upload validation
    max_upload_size_bytes: int = 25 * 1024 * 1024  # 25 MB
    allowed_content_types: list[str] = [
        "application/pdf",
        "text/plain",
        "text/csv",
        "application/json",
        "application/octet-stream",
    ]

    # TTL retention in seconds (30 days default)
    job_ttl_seconds: int = 30 * 24 * 60 * 60

    @property
    def is_local(self) -> bool:
        """True when running against LocalStack, False for real AWS."""
        return self.environment == "local"


# Module-level singleton — import and use directly
settings = Settings()


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()

