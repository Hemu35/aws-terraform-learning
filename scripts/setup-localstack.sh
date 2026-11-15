#!/bin/bash
# setup-localstack.sh
# Bootstraps AWS resources in LocalStack.
# Can be run standalone (export AWS_ENDPOINT_URL=http://localhost:4566 first)
# or as the entrypoint of the localstack-setup container.

set -e

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "[setup] Waiting for LocalStack to be ready..."
sleep 5

echo "[setup] Using endpoint: $ENDPOINT"

# ── S3 Bucket ────────────────────────────────────────────────────────────────
echo "[setup] Creating S3 bucket: documents"
aws s3api create-bucket \
  --bucket documents \
  --region "$REGION" \
  --endpoint-url "$ENDPOINT" || echo "[setup] Bucket 'documents' already exists, skipping."

# ── DynamoDB Table ───────────────────────────────────────────────────────────
echo "[setup] Creating DynamoDB table: jobs"
aws dynamodb create-table \
  --table-name jobs \
  --attribute-definitions AttributeName=job_id,AttributeType=S \
  --key-schema AttributeName=job_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" \
  --endpoint-url "$ENDPOINT" || echo "[setup] Table 'jobs' already exists, skipping."

# ── SQS Queue ────────────────────────────────────────────────────────────────
echo "[setup] Creating SQS queue: jobs-queue"
aws sqs create-queue \
  --queue-name jobs-queue \
  --region "$REGION" \
  --endpoint-url "$ENDPOINT" || echo "[setup] Queue 'jobs-queue' already exists, skipping."

echo "[setup] All LocalStack resources are ready."
