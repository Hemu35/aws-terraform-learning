variable "cluster_name" {
  description = "Name of the ECS cluster"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "project_name" {
  description = "Name of the project — used for tagging and naming"
  type        = string
}

variable "api_image" {
  description = "Docker image URI for the API container (e.g. 123456789.dkr.ecr.us-east-1.amazonaws.com/api:latest)"
  type        = string
}

variable "worker_image" {
  description = "Docker image URI for the worker container"
  type        = string
}

variable "task_role_arn" {
  description = "Fallback ARN of the IAM role for tasks (backward-compatible alias)"
  type        = string
  default     = ""
}

variable "api_task_role_arn" {
  description = "ARN of the IAM role the API task code assumes"
  type        = string
  default     = ""
}

variable "worker_task_role_arn" {
  description = "ARN of the IAM role the Worker task code assumes"
  type        = string
  default     = ""
}

variable "api_desired_count" {
  description = "Desired number of API tasks"
  type        = number
  default     = 1
}

variable "worker_desired_count" {
  description = "Desired number of Worker tasks"
  type        = number
  default     = 1
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs for the ALB"
  type        = list(string)
  default     = []
}

variable "sqs_queue_name" {
  description = "Name of the SQS queue for worker autoscaling metrics"
  type        = string
  default     = ""
}

variable "execution_role_arn" {
  description = "ARN of the IAM role ECS uses to pull images and ship logs"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where ECS tasks run"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs (public subnets) for the ECS service"
  type        = list(string)
}

variable "app_environment_vars" {
  description = "Environment variables injected into both containers as a map of name → value"
  type        = map(string)
  default     = {}
}

variable "aws_region" {
  description = "AWS region — needed to build CloudWatch log group ARNs and awslogs config"
  type        = string
}
