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
  description = "Fallback subnet IDs for backwards compatibility"
  type        = list(string)
  default     = ["subnet-placeholder-1", "subnet-placeholder-2"]
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs for the ALB (public ingress)"
  type        = list(string)
  default     = ["subnet-placeholder-pub-1", "subnet-placeholder-pub-2"]
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for ECS tasks (API and Worker) with assign_public_ip = false"
  type        = list(string)
  default     = ["subnet-placeholder-priv-1", "subnet-placeholder-priv-2"]
}

variable "private_route_table_ids" {
  description = "List of private route table IDs for S3 and DynamoDB VPC Gateway Endpoints"
  type        = list(string)
  default     = []
}

variable "certificate_arn" {
  description = "ACM Certificate ARN for ALB HTTPS listener on port 443 (optional)"
  type        = string
  default     = ""
}

variable "enable_vpc_endpoints" {
  description = "Whether to provision VPC Endpoints for private subnet isolation"
  type        = bool
  default     = true
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
