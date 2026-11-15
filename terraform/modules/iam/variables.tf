variable "project_name" {
  description = "Name of the project — used for naming IAM roles"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket the ECS task role may access"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS queue the ECS task role may access"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table the ECS task role may access"
  type        = string
}
