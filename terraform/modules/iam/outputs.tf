output "api_task_role_arn" {
  description = "ARN of the ECS API task role (least privilege for API service)"
  value       = aws_iam_role.api_task_role.arn
}

output "worker_task_role_arn" {
  description = "ARN of the ECS Worker task role (least privilege for Worker service)"
  value       = aws_iam_role.worker_task_role.arn
}

output "task_role_arn" {
  description = "ARN of the ECS task role (backward-compatible alias for api_task_role)"
  value       = aws_iam_role.api_task_role.arn
}

output "execution_role_arn" {
  description = "ARN of the ECS execution role (ECS control-plane permissions)"
  value       = aws_iam_role.ecs_execution_role.arn
}
