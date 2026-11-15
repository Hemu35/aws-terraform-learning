"""
AWS Client Factory
==================
Provides a unified factory function for creating boto3 clients that works
both locally (via LocalStack) and against real AWS.

Design principles:
- endpoint_url is injected only when settings.aws_endpoint_url is set,
  which is non-None only in local/test environments (LocalStack).
- Credentials are always sourced from the settings object (which reads from
  environment variables via pydantic-settings). They are NEVER hardcoded.
- In production (settings.aws_endpoint_url is None), boto3 falls through to
  its standard credential chain (IAM role / environment / ~/.aws/credentials).
"""

from __future__ import annotations

from typing import Any

import boto3


def get_boto3_client(service: str, settings: Any) -> "boto3.client":
    """Create and return a boto3 service client.

    Parameters
    ----------
    service:
        AWS service name understood by boto3, e.g. ``"s3"``, ``"sqs"``.
    settings:
        Application settings object (``app.core.config.Settings``).
        Must expose: ``aws_default_region``, ``aws_endpoint_url`` (Optional[str]),
        ``aws_access_key_id``, ``aws_secret_access_key``.

    Returns
    -------
    boto3.client
        A configured boto3 service client.
    """
    kwargs: dict[str, Any] = {
        "region_name": settings.aws_default_region,
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
    }

    # Only inject endpoint_url for LocalStack / local dev; never in production.
    if settings.aws_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.aws_endpoint_url

    return boto3.client(service, **kwargs)


def get_boto3_resource(service: str, settings: Any) -> "boto3.resource":
    """Create and return a boto3 service resource (higher-level API).

    Parameters
    ----------
    service:
        AWS service name, e.g. ``"dynamodb"``.
    settings:
        Application settings object.

    Returns
    -------
    boto3.resource
        A configured boto3 service resource.
    """
    kwargs: dict[str, Any] = {
        "region_name": settings.aws_default_region,
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
    }

    if settings.aws_endpoint_url is not None:
        kwargs["endpoint_url"] = settings.aws_endpoint_url

    return boto3.resource(service, **kwargs)
