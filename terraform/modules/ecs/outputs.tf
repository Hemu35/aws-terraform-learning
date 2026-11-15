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
