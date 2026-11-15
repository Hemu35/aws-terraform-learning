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

# ── ECS Task Role (application code permissions) ─────────────
resource "aws_iam_role" "ecs_task_role" {
  name               = "${var.project_name}-${var.environment}-ecs-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_iam_policy_document" "ecs_task_permissions" {
  # S3 — read input documents, write results
  statement {
    sid    = "S3DocumentAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      "${var.s3_bucket_arn}/*", # Scoped to objects inside this bucket only
    ]
  }

  # SQS — produce and consume job messages
  statement {
    sid    = "SQSJobQueueAccess"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility", # Required by heartbeat to extend visibility & prevent redelivery loops
      "sqs:GetQueueAttributes",      # Required by boto3 SDK for queue introspection
    ]
    resources = [var.sqs_queue_arn]
  }

  # DynamoDB — read and write job state records
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

resource "aws_iam_policy" "ecs_task_policy" {
  name        = "${var.project_name}-${var.environment}-ecs-task-policy"
  description = "Least-privilege policy for the ${var.project_name} ECS task (${var.environment})"
  policy      = data.aws_iam_policy_document.ecs_task_permissions.json
}

resource "aws_iam_role_policy_attachment" "ecs_task_policy" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.ecs_task_policy.arn
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
