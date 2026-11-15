# ============================================================
# ECS Module — Fargate Cluster, Task Definition & Service
# ============================================================
# WHY Fargate?
#   Fargate removes EC2 instance management. No patching, no
#   cluster sizing, no SSH. For a document processing API you
#   pay per-vCPU/memory-second rather than idle instance time —
#   cost-efficient at dev/low-traffic scale and simpler ops.
#
# WHY 256 CPU / 512 MB?
#   Smallest Fargate allocation. Enough to run FastAPI + a worker
#   loop. Keeps costs near ~$0.01/hr in dev. Scale up for prod.
#
# WHY two containers in one task (api + worker)?
#   They share environment variables and lifecycle. In production
#   you might separate them into distinct services for independent
#   scaling, but for a dev/learning environment co-locating is
#   simpler and uses fewer Fargate tasks.
#
# WHY awslogs driver?
#   CloudWatch is the native AWS log aggregator — zero config
#   beyond specifying the log group. Required for observability
#   in a production-grade setup; demonstrates logging awareness.
#
# WHY 1 desired count with public subnets?
#   Dev environment — 1 task minimises cost. Public subnets allow
#   outbound internet access without a NAT Gateway (~$32/mo each).
#   For prod, place in private subnets + NAT/VPC endpoints.
# ============================================================

locals {
  # Build the environment variable list that ECS task definitions expect:
  # [{ name = "KEY", value = "VALUE" }, ...]
  app_env_list = [
    for k, v in var.app_environment_vars : { name = k, value = v }
  ]

  log_group_name = "/ecs/${var.project_name}/${var.environment}"
}

# CloudWatch log group — created here so the task definition can reference it
resource "aws_cloudwatch_log_group" "ecs" {
  name              = local.log_group_name
  retention_in_days = 7 # Keep 7 days in dev; increase for prod compliance

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ECS Cluster — Fargate capacity provider requires no EC2 instances
resource "aws_ecs_cluster" "main" {
  name = var.cluster_name

  setting {
    name  = "containerInsights"
    value = "disabled" # Container Insights costs extra (~$0.35/task/month); disable in dev
  }

  tags = {
    Name        = var.cluster_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Task Definition — defines both the api and worker containers
resource "aws_ecs_task_definition" "app" {
  family                   = "${var.project_name}-${var.environment}"
  network_mode             = "awsvpc" # Required for Fargate
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256" # 0.25 vCPU — smallest Fargate unit
  memory                   = "512" # 512 MB — sufficient for FastAPI + worker
  task_role_arn            = var.task_role_arn
  execution_role_arn       = var.execution_role_arn

  container_definitions = jsonencode([
    # ── API container ─────────────────────────────────────────
    {
      name      = "api"
      image     = var.api_image
      essential = true # ECS stops the task if this container exits

      portMappings = [
        {
          containerPort = 8000
          protocol      = "tcp"
        }
      ]

      environment = local.app_env_list

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = local.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "api"
        }
      }
    },
    # ── Worker container ──────────────────────────────────────
    {
      name      = "worker"
      image     = var.worker_image
      essential = false # Task can survive if the worker crashes (API still serves health checks)

      environment = local.app_env_list

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = local.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "worker"
        }
      }
    }
  ])

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Security group — allow inbound on port 8000 (API), all outbound
resource "aws_security_group" "ecs_service" {
  name        = "${var.project_name}-${var.environment}-ecs-sg"
  description = "Allow inbound to ECS API container on port 8000"
  vpc_id      = var.vpc_id

  ingress {
    description = "FastAPI port"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Tighten to ALB SG in production
  }

  egress {
    description = "Allow all outbound - needed for ECR pull, SQS, S3, DynamoDB calls"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-ecs-sg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ECS Service — keeps 1 task running; Fargate handles placement
resource "aws_ecs_service" "api" {
  name            = "${var.project_name}-${var.environment}-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.app.arn
  desired_count   = 1 # 1 task in dev; scale horizontally in prod
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.ecs_service.id]
    assign_public_ip = true # Public IP avoids NAT Gateway cost in dev
  }

  # Allow rolling deploys without downtime even at desired_count=1
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 200

  tags = {
    Name        = "${var.project_name}-${var.environment}-api"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
