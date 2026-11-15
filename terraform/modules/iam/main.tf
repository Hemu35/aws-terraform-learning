# ============================================================
# IAM Module — Least-Privilege Roles for ECS
# ============================================================
# WHY two separate roles?
#   AWS best practice separates the EXECUTION role (ECS control-
#   plane permissions: pull image, write logs) from the TASK role
#   (application permissions: S3, SQS, DynamoDB). If the app is
#   compromised it cannot pull new container images or modify ECS.
#
# WHY no wildcards in the task role?
#   Principle of Least Privilege. The task role grants access only
#   to the specific S3 bucket, SQS queue, and DynamoDB table this
#   deployment owns — not every bucket/queue/table in the account.
#   This is a frequent interview security question.
#
# WHY AmazonECSTaskExecutionRolePolicy for the execution role?
#   It's the AWS-managed policy maintained by AWS covering exactly
#   what ECS needs: ECR auth, CloudWatch log creation, Secrets
#   Manager read (for future use). Using the managed policy means
#   AWS keeps it up to date as the service evolves.
#
# WHY sts:AssumeRole from ecs-tasks.amazonaws.com?
#   Both roles must have a trust policy allowing the ECS service
#   principal to assume them. Without this the roles exist but ECS
#   cannot use them and task launches fail.
# ============================================================

# ── Trust policy shared by both roles ────────────────────────
data "aws_iam_policy_document" "ecs_trust" {
  statement {
    sid     = "AllowECSTasksToAssume"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ── ECS API Task Role (API application permissions) ─────────
resource "aws_iam_role" "api_task_role" {
  name               = "${var.project_name}-${var.environment}-ecs-api-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Service     = "api"
  }
}

data "aws_iam_policy_document" "api_task_permissions" {
  # S3 — upload input documents and generate presigned URLs
  statement {
    sid    = "S3DocumentAccess"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
    ]
    resources = [
      "${var.s3_bucket_arn}/*",
    ]
  }

  # SQS — produce/enqueue job messages only
  statement {
    sid    = "SQSJobQueueProduceAccess"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
    ]
    resources = [var.sqs_queue_arn]
  }

  # DynamoDB — create job records, poll status, and update input keys
  statement {
    sid    = "DynamoDBJobTableAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [var.dynamodb_table_arn]
  }
}

resource "aws_iam_policy" "api_task_policy" {
  name        = "${var.project_name}-${var.environment}-ecs-api-task-policy"
  description = "Least-privilege policy for the ${var.project_name} API task (${var.environment})"
  policy      = data.aws_iam_policy_document.api_task_permissions.json
}

resource "aws_iam_role_policy_attachment" "api_task_policy" {
  role       = aws_iam_role.api_task_role.name
  policy_arn = aws_iam_policy.api_task_policy.arn
}

# ── ECS Worker Task Role (Worker application permissions) ────
resource "aws_iam_role" "worker_task_role" {
  name               = "${var.project_name}-${var.environment}-ecs-worker-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Service     = "worker"
  }
}

data "aws_iam_policy_document" "worker_task_permissions" {
  # S3 — read input documents and write processing results
  statement {
    sid    = "S3DocumentProcessAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      "${var.s3_bucket_arn}/*",
    ]
  }

  # SQS — consume, delete, and extend visibility for job messages
  statement {
    sid    = "SQSJobQueueConsumeAccess"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [var.sqs_queue_arn]
  }

  # DynamoDB — query job items and atomically update lease/status transitions
  statement {
    sid    = "DynamoDBJobTableLeaseAccess"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
    ]
    resources = [var.dynamodb_table_arn]
  }
}

resource "aws_iam_policy" "worker_task_policy" {
  name        = "${var.project_name}-${var.environment}-ecs-worker-task-policy"
  description = "Least-privilege policy for the ${var.project_name} Worker task (${var.environment})"
  policy      = data.aws_iam_policy_document.worker_task_permissions.json
}

resource "aws_iam_role_policy_attachment" "worker_task_policy" {
  role       = aws_iam_role.worker_task_role.name
  policy_arn = aws_iam_policy.worker_task_policy.arn
}

# ── ECS Execution Role (ECS control-plane permissions) ───────
resource "aws_iam_role" "ecs_execution_role" {
  name               = "${var.project_name}-${var.environment}-ecs-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# AWS-managed policy — covers ECR pull, CloudWatch log creation, Secrets Manager read
resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
