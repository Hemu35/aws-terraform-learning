# ============================================================
# Dev Environment — Document Processing System
# ============================================================
# ARCHITECTURAL DECISIONS & INTERVIEW HIGHLIGHTS:
#
# 1. Cost Efficiency (Dev Tier):
#    - DynamoDB PAY_PER_REQUEST: Zero cost when idle; ideal for dev/learning.
#    - S3 SSE-S3 (AES256): Avoids AWS KMS request charges (~$0.03/10k calls).
#    - S3 Lifecycle Rule: Automatically expires input documents after 30 days.
#    - ECS Fargate 256 CPU / 512 MB: Lowest tier compute (~$0.01/hr).
#    - Fargate Public Subnet Deployment: Avoids AWS NAT Gateway (~$32+/month per AZ).
#    - CloudWatch 7-day retention: Limits dev log storage overhead.
#
# 2. Security & Least Privilege:
#    - Strict IAM separation: Execution role (ECR pull, logs) separated from
#      Task role (business logic permissions).
#    - Zero Wildcard Policies: Task role is explicitly scoped to this specific
#      bucket, queue, and DynamoDB table.
#    - Public Access Block: S3 bucket explicitly blocks all public reads/writes.
#
# 3. Resilience & Production Readiness:
#    - SQS Dead-Letter Queue (DLQ): Auto-routes poisoned/failing messages
#      after 3 failed attempts with 300s visibility timeout for batch processing.
#    - DynamoDB TTL: Automatic cleanup of expired jobs without manual scripts.
# ============================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ------------------------------------------------------------
# Module 1: S3 Document Store
# ------------------------------------------------------------
module "s3" {
  source = "../../modules/s3"

  bucket_name  = "${var.project_name}-${var.environment}-documents"
  project_name = var.project_name
  environment  = var.environment
}

# ------------------------------------------------------------
# Module 2: SQS Message & Dead-Letter Queues
# ------------------------------------------------------------
module "sqs" {
  source = "../../modules/sqs"

  queue_name   = "${var.project_name}-${var.environment}-jobs-queue"
  project_name = var.project_name
  environment  = var.environment
}

# ------------------------------------------------------------
# Module 3: DynamoDB Job Metadata Store
# ------------------------------------------------------------
module "dynamodb" {
  source = "../../modules/dynamodb"

  table_name   = "${var.project_name}-${var.environment}-jobs"
  project_name = var.project_name
  environment  = var.environment
}

# ------------------------------------------------------------
# Module 4: IAM Roles & Policies (Least Privilege)
# ------------------------------------------------------------
module "iam" {
  source = "../../modules/iam"

  project_name       = var.project_name
  environment        = var.environment
  s3_bucket_arn      = module.s3.bucket_arn
  sqs_queue_arn      = module.sqs.queue_arn
  dynamodb_table_arn = module.dynamodb.table_arn
}

# ------------------------------------------------------------
# Module 5: ECS Fargate Cluster & Microservices
# ------------------------------------------------------------
module "ecs" {
  source = "../../modules/ecs"

  cluster_name            = "${var.project_name}-${var.environment}-cluster"
  project_name            = var.project_name
  environment             = var.environment
  aws_region              = var.aws_region
  api_image               = var.api_image
  worker_image            = var.worker_image
  api_task_role_arn       = module.iam.api_task_role_arn
  worker_task_role_arn    = module.iam.worker_task_role_arn
  execution_role_arn      = module.iam.execution_role_arn
  vpc_id                  = var.vpc_id
  subnet_ids              = var.subnet_ids
  public_subnet_ids       = var.public_subnet_ids
  private_subnet_ids      = var.private_subnet_ids
  private_route_table_ids = var.private_route_table_ids
  enable_vpc_endpoints    = var.enable_vpc_endpoints
  certificate_arn         = var.certificate_arn
  sqs_queue_name          = module.sqs.queue_name

  # Injected directly into container task definitions
  app_environment_vars = {
    ENVIRONMENT         = var.environment
    LOG_LEVEL           = "INFO"
    AWS_DEFAULT_REGION  = var.aws_region
    S3_BUCKET_NAME      = module.s3.bucket_id
    SQS_QUEUE_URL       = module.sqs.queue_url
    DYNAMODB_TABLE_NAME = module.dynamodb.table_name
  }
}
