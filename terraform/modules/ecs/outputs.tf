output "cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "service_name" {
  description = "Name of the ECS service running the API container"
  value       = aws_ecs_service.api.name
}

output "task_definition_arn" {
  description = "ARN of the ECS task definition (includes revision)"
  value       = aws_ecs_task_definition.app.arn
}
