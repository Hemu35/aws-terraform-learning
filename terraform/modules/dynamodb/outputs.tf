output "table_name" {
  description = "Name of the DynamoDB table — referenced in app config"
  value       = aws_dynamodb_table.jobs.name
}

output "table_arn" {
  description = "ARN of the DynamoDB table — used in IAM policies"
  value       = aws_dynamodb_table.jobs.arn
}
