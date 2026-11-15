# ============================================================
# S3 Module — Document Storage
# ============================================================
# WHY S3?
#   S3 is the canonical AWS object store: infinitely scalable,
#   eleven-nines durability, and cheap at ~$0.023/GB/month.
#   For a document processing pipeline it's the natural landing
#   zone for both raw uploads and processed results.
#
# WHY versioning?
#   Protects against accidental overwrites and enables audit
#   trails for compliance — a common interview topic.
#
# WHY AES256 (SSE-S3) over KMS?
#   SSE-S3 is free; KMS costs $0.03/10k requests. For a dev/
#   learning environment this avoids surprise bills while still
#   demonstrating encryption-at-rest best practice.
#
# WHY block all public access?
#   Documents may contain PII. Blocking public access is the
#   AWS-recommended default; it must be explicit in Terraform.
#
# WHY the lifecycle rule (30-day expiry)?
#   Input documents are only needed during processing. Expiring
#   them cuts storage cost automatically — shows cost-awareness.
# ============================================================

resource "aws_s3_bucket" "documents" {
  bucket = var.bucket_name

  tags = {
    Name        = var.bucket_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Versioning — protects against accidental deletes / overwrites
resource "aws_s3_bucket_versioning" "documents" {
  bucket = aws_s3_bucket.documents.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block every avenue of public exposure
resource "aws_s3_bucket_public_access_block" "documents" {
  bucket = aws_s3_bucket.documents.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Server-side encryption — AES256 (SSE-S3) is free and sufficient
resource "aws_s3_bucket_server_side_encryption_configuration" "documents" {
  bucket = aws_s3_bucket.documents.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle rule — expire raw input files after 30 days to save cost
resource "aws_s3_bucket_lifecycle_configuration" "documents" {
  bucket = aws_s3_bucket.documents.id

  rule {
    id     = "expire-documents-after-30-days"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }

    # Also expire old non-current versions after 30 days
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
