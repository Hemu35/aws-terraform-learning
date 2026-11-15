output "task_role_arn" {
  description = "ARN of the ECS task role (application code permissions)"
  value       = aws_iam_role.ecs_task_role.arn
}

output "execution_role_arn" {
  description = "ARN of the ECS execution role (ECS control-plane permissions)"
  value       = aws_iam_role.ecs_execution_role.arn
}
