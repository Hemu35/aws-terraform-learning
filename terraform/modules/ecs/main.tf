# ============================================================
# ECS Module — Fargate Cluster, Microservices (API + Worker),
# Application Load Balancer & Secure Network Access
# ============================================================
# ARCHITECTURE DECISIONS:
#
# 1. Microservice Separation:
#    - API and Worker are separated into distinct ECS task definitions
#      and services.
#    - API scales on HTTP traffic/ALB metrics; Worker container is
#      essential = true and auto-scales on SQS backlog metrics.
#    - A worker crash causes ECS to automatically launch a replacement
#      task immediately without impacting the API service.
#
# 2. Network Security & Least Privilege:
#    - ALB Security Group: Ingress on port 80 (HTTP) from 0.0.0.0/0.
#    - API Security Group: Ingress on port 8000 ONLY from the ALB security
#      group (no public ingress!).
#    - Worker Security Group: ZERO inbound ports permitted. Outbound only.
#
# 3. Observability & Graceful Shutdown:
#    - CloudWatch log groups with structured logging per service.
#    - ALB Target Group health check on /health.
#    - stopTimeout configured for graceful SIGTERM processing.
# ============================================================

locals {
  app_env_list = [
    for k, v in var.app_environment_vars : { name = k, value = v }
  ]

  log_group_name   = "/ecs/${var.project_name}/${var.environment}"
  api_task_role    = var.api_task_role_arn != "" ? var.api_task_role_arn : var.task_role_arn
  worker_task_role = var.worker_task_role_arn != "" ? var.worker_task_role_arn : var.task_role_arn
  alb_subnets      = length(var.public_subnet_ids) > 0 ? var.public_subnet_ids : var.subnet_ids
  task_subnets     = length(var.private_subnet_ids) > 0 ? var.private_subnet_ids : var.subnet_ids
}

# CloudWatch log group
resource "aws_cloudwatch_log_group" "ecs" {
  name              = local.log_group_name
  retention_in_days = 7

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = var.cluster_name

  setting {
    name  = "containerInsights"
    value = "disabled"
  }

  tags = {
    Name        = var.cluster_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ============================================================
# Security Groups: Tiered Ingress & Egress Isolation
# ============================================================

# 1. ALB Security Group — accepts public HTTP on port 80
resource "aws_security_group" "alb" {
  name        = "${var.project_name}-${var.environment}-alb-sg"
  description = "Public ingress for Application Load Balancer"
  vpc_id      = var.vpc_id

  ingress {
    description = "Public HTTP ingress"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "Public HTTPS ingress"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Forward traffic to API tasks"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-alb-sg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 2. API Security Group — accepts traffic ONLY from the ALB security group
resource "aws_security_group" "api" {
  name        = "${var.project_name}-${var.environment}-api-sg"
  description = "Private ingress for API tasks from ALB only"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Allow port 8000 only from ALB"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    description = "Outbound access for AWS APIs (DynamoDB, SQS, S3, ECR, CloudWatch)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-api-sg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 3. Worker Security Group — ZERO inbound ports
resource "aws_security_group" "worker" {
  name        = "${var.project_name}-${var.environment}-worker-sg"
  description = "Isolated security group for background worker tasks (zero ingress)"
  vpc_id      = var.vpc_id

  egress {
    description = "Outbound access for AWS APIs (DynamoDB, SQS, S3, ECR, CloudWatch)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-worker-sg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ============================================================
# Application Load Balancer & Target Group
# ============================================================

resource "aws_lb" "main" {
  name               = "${var.project_name}-${var.environment}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = local.alb_subnets

  tags = {
    Name        = "${var.project_name}-${var.environment}-alb"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_lb_target_group" "api" {
  name        = "${var.project_name}-${var.environment}-api-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  health_check {
    path                = "/health"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-api-tg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  dynamic "default_action" {
    for_each = var.certificate_arn != "" ? [1] : []
    content {
      type = "redirect"

      redirect {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }
  }

  dynamic "default_action" {
    for_each = var.certificate_arn == "" ? [1] : []
    content {
      type             = "forward"
      target_group_arn = aws_lb_target_group.api.arn
    }
  }
}

resource "aws_lb_listener" "https" {
  count             = var.certificate_arn != "" ? 1 : 0
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}

# ============================================================
# Dedicated Task Definitions
# ============================================================

# 1. API Task Definition
resource "aws_ecs_task_definition" "api" {
  family                   = "${var.project_name}-${var.environment}-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  task_role_arn            = local.api_task_role
  execution_role_arn       = var.execution_role_arn

  container_definitions = jsonencode([
    {
      name        = "api"
      image       = var.api_image
      essential   = true
      stopTimeout = 30

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
    }
  ])

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 2. Worker Task Definition
resource "aws_ecs_task_definition" "worker" {
  family                   = "${var.project_name}-${var.environment}-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  task_role_arn            = local.worker_task_role
  execution_role_arn       = var.execution_role_arn

  container_definitions = jsonencode([
    {
      name        = "worker"
      image       = var.worker_image
      essential   = true
      stopTimeout = 30

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

# ============================================================
# Dedicated ECS Services
# ============================================================

# 1. API ECS Service (attached to ALB)
resource "aws_ecs_service" "api" {
  name            = "${var.project_name}-${var.environment}-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.task_subnets
    security_groups  = [aws_security_group.api.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8000
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  tags = {
    Name        = "${var.project_name}-${var.environment}-api"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 2. Worker ECS Service (Background queue processor, zero public ports)
resource "aws_ecs_service" "worker" {
  name            = "${var.project_name}-${var.environment}-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.task_subnets
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = false
  }

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  tags = {
    Name        = "${var.project_name}-${var.environment}-worker"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ============================================================
# Worker Autoscaling (Target Tracking / Scale on SQS Backlog)
# ============================================================

resource "aws_appautoscaling_target" "worker" {
  max_capacity       = 5
  min_capacity       = 1
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "worker_scale_out" {
  name               = "${var.project_name}-${var.environment}-worker-scale-out"
  policy_type        = "StepScaling"
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  service_namespace  = aws_appautoscaling_target.worker.service_namespace

  step_scaling_policy_configuration {
    adjustment_type         = "ChangeInCapacity"
    cooldown                = 60
    metric_aggregation_type = "Maximum"

    step_adjustment {
      metric_interval_lower_bound = 0
      scaling_adjustment          = 1
    }
  }
}

# CloudWatch alarm to trigger worker scale out when SQS queue has messages
resource "aws_cloudwatch_metric_alarm" "worker_backlog" {
  alarm_name          = "${var.project_name}-${var.environment}-worker-backlog"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 5
  alarm_description   = "Scale out workers when SQS queue backlog exceeds 5 messages"

  dimensions = {
    QueueName = var.sqs_queue_name != "" ? var.sqs_queue_name : "${var.project_name}-${var.environment}-jobs-queue"
  }

  alarm_actions = [aws_appautoscaling_policy.worker_scale_out.arn]

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ============================================================
# CloudWatch Alarms for Service Health & Resource Utilization
# ============================================================

resource "aws_cloudwatch_metric_alarm" "api_cpu_high" {
  alarm_name          = "${var.project_name}-${var.environment}-api-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = 60
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Alarm when API ECS service CPU exceeds 80% for 2 consecutive minutes"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.api.name
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_cloudwatch_metric_alarm" "worker_cpu_high" {
  alarm_name          = "${var.project_name}-${var.environment}-worker-cpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ECS"
  period              = 60
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Alarm when Worker ECS service CPU exceeds 80% for 2 consecutive minutes"
  treat_missing_data  = "notBreaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.main.name
    ServiceName = aws_ecs_service.worker.name
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ============================================================
# VPC Endpoints: Network Isolation without NAT Gateway
# ============================================================

# 1. Gateway Endpoints: Free of charge, route-table based
resource "aws_vpc_endpoint" "s3" {
  count             = var.enable_vpc_endpoints ? 1 : 0
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.private_route_table_ids

  tags = {
    Name        = "${var.project_name}-${var.environment}-s3-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_vpc_endpoint" "dynamodb" {
  count             = var.enable_vpc_endpoints ? 1 : 0
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.private_route_table_ids

  tags = {
    Name        = "${var.project_name}-${var.environment}-dynamodb-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 2. Interface Endpoints Security Group (inbound port 443 from API and Worker tasks)
resource "aws_security_group" "vpc_endpoints" {
  count       = var.enable_vpc_endpoints ? 1 : 0
  name        = "${var.project_name}-${var.environment}-vpce-sg"
  description = "Security group for VPC Interface Endpoints (SQS, ECR, CloudWatch Logs)"
  vpc_id      = var.vpc_id

  ingress {
    description     = "Allow HTTPS from API and Worker tasks"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.api.id, aws_security_group.worker.id]
  }

  egress {
    description = "Allow all outbound traffic from interface endpoints"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-vpce-sg"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# 3. Interface Endpoints: Private AWS API connectivity for tasks in private subnets
resource "aws_vpc_endpoint" "sqs" {
  count               = var.enable_vpc_endpoints ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.sqs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = local.task_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints[0].id]

  tags = {
    Name        = "${var.project_name}-${var.environment}-sqs-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_vpc_endpoint" "ecr_api" {
  count               = var.enable_vpc_endpoints ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.api"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = local.task_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints[0].id]

  tags = {
    Name        = "${var.project_name}-${var.environment}-ecr-api-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  count               = var.enable_vpc_endpoints ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = local.task_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints[0].id]

  tags = {
    Name        = "${var.project_name}-${var.environment}-ecr-dkr-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_vpc_endpoint" "logs" {
  count               = var.enable_vpc_endpoints ? 1 : 0
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = local.task_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints[0].id]

  tags = {
    Name        = "${var.project_name}-${var.environment}-logs-vpce"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
