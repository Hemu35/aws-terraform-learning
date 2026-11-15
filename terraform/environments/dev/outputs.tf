# ============================================================
# Dev Environment Outputs
# ============================================================

output "s3_bucket_id" {
  description = "Document storage S3 bucket name"
  value       = module.s3.bucket_id
}

output "s3_bucket_arn" {
  description = "Document storage S3 bucket ARN"
  value       = module.s3.bucket_arn
}

output "sqs_queue_url" {
  description = "SQS job processing queue URL"
  value       = module.sqs.queue_url
}

output "sqs_queue_arn" {
  description = "SQS job processing queue ARN"
  value       = module.sqs.queue_arn
}

output "sqs_dlq_url" {
  description = "SQS Dead-Letter Queue URL"
  value       = module.sqs.dlq_url
}

output "sqs_dlq_arn" {
  description = "SQS Dead-Letter Queue ARN"
  value       = module.sqs.dlq_arn
}

output "dynamodb_table_name" {
  description = "DynamoDB table name for document jobs"
  value       = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  description = "DynamoDB table ARN"
  value       = module.dynamodb.table_arn
}

output "ecs_task_role_arn" {
  description = "IAM Role ARN assumed by ECS application containers"
  value       = module.iam.task_role_arn
}

output "ecs_execution_role_arn" {
  description = "IAM Execution Role ARN used by ECS agent to pull images and configure logging"
  value       = module.iam.execution_role_arn
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = module.ecs.cluster_arn
}

output "ecs_service_name" {
  description = "ECS API service name"
  value       = module.ecs.service_name
}

output "ecs_api_service_name" {
  description = "ECS API service name"
  value       = module.ecs.api_service_name
}

output "ecs_worker_service_name" {
  description = "ECS Worker service name"
  value       = module.ecs.worker_service_name
}

output "alb_dns_name" {
  description = "Application Load Balancer DNS name"
  value       = module.ecs.alb_dns_name
}

output "ecs_task_definition_arn" {
  description = "ECS API task definition ARN"
  value       = module.ecs.task_definition_arn
}
