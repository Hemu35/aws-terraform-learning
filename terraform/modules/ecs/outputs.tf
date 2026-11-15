output "cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "api_service_name" {
  description = "Name of the ECS service running the API container"
  value       = aws_ecs_service.api.name
}

output "worker_service_name" {
  description = "Name of the ECS service running the Worker container"
  value       = aws_ecs_service.worker.name
}

output "service_name" {
  description = "Backward-compatible alias for api_service_name"
  value       = aws_ecs_service.api.name
}

output "alb_dns_name" {
  description = "Public DNS name of the Application Load Balancer"
  value       = aws_lb.main.dns_name
}

output "alb_arn" {
  description = "ARN of the Application Load Balancer"
  value       = aws_lb.main.arn
}

output "api_task_definition_arn" {
  description = "ARN of the API ECS task definition"
  value       = aws_ecs_task_definition.api.arn
}

output "worker_task_definition_arn" {
  description = "ARN of the Worker ECS task definition"
  value       = aws_ecs_task_definition.worker.arn
}

output "task_definition_arn" {
  description = "Backward-compatible alias for api_task_definition_arn"
  value       = aws_ecs_task_definition.api.arn
}

output "alb_https_listener_arn" {
  description = "ARN of the HTTPS ALB listener on port 443 (null if certificate_arn was not provided)"
  value       = try(aws_lb_listener.https[0].arn, null)
}

output "vpc_endpoints_security_group_id" {
  description = "Security group ID for VPC Interface Endpoints"
  value       = try(aws_security_group.vpc_endpoints[0].id, null)
}

output "s3_vpc_endpoint_id" {
  description = "ID of the S3 Gateway VPC Endpoint"
  value       = try(aws_vpc_endpoint.s3[0].id, null)
}

output "dynamodb_vpc_endpoint_id" {
  description = "ID of the DynamoDB Gateway VPC Endpoint"
  value       = try(aws_vpc_endpoint.dynamodb[0].id, null)
}

output "sqs_vpc_endpoint_id" {
  description = "ID of the SQS Interface VPC Endpoint"
  value       = try(aws_vpc_endpoint.sqs[0].id, null)
}
