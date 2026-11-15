variable "aws_region" {
  description = "AWS region for provisioning resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix used for resource naming and tagging"
  type        = string
  default     = "doc-processor"
}

variable "environment" {
  description = "Target deployment environment"
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC ID where the ECS service will be launched (required for real AWS deployment)"
  type        = string
  default     = "vpc-placeholder"
}

variable "subnet_ids" {
  description = "List of public subnet IDs for ECS tasks (required for real AWS deployment)"
  type        = list(string)
  default     = ["subnet-placeholder-1", "subnet-placeholder-2"]
}

variable "api_image" {
  description = "Container image URI for the FastAPI application"
  type        = string
  default     = "doc-processor-api:latest"
}

variable "worker_image" {
  description = "Container image URI for the background worker"
  type        = string
  default     = "doc-processor-worker:latest"
}
