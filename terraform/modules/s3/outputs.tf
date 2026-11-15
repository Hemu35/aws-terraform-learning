output "bucket_id" {
  description = "The name (ID) of the S3 bucket"
  value       = aws_s3_bucket.documents.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket — used in IAM policy conditions"
  value       = aws_s3_bucket.documents.arn
}
