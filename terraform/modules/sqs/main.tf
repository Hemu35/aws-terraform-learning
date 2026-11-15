# ============================================================
# SQS Module — Job Queue + Dead-Letter Queue
# ============================================================
# WHY SQS?
#   SQS decouples the API (producer) from the worker (consumer).
#   The API can return 202 immediately; the worker processes
#   asynchronously. This is the standard fan-out pattern for
#   document pipelines and a core interview talking point.
#
# WHY a Dead-Letter Queue (DLQ)?
#   If a message fails to process 3 times it's moved to the DLQ
#   instead of being lost. Ops/on-call can inspect DLQ messages
#   to understand failures — demonstrates production-readiness.
#
# WHY 300s visibility timeout?
#   Document processing (OCR, NLP, etc.) can take minutes.
#   The visibility timeout must be longer than the worst-case
#   processing time so the worker isn't pre-empted mid-job.
#
# WHY 4-day retention?
#   SQS max is 14 days; 4 days is a safe window to investigate
#   failures without accumulating cost or stale messages.
#
# WHY standard queue (not FIFO)?
#   Document jobs are independent — ordering doesn't matter.
#   Standard queues have higher throughput and lower cost.
# ============================================================

# Dead-Letter Queue — receives messages after 3 failed attempts
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.queue_name}-dlq"
  message_retention_seconds = 345600 # 4 days — time to investigate failures

  tags = {
    Name        = "${var.queue_name}-dlq"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Main job queue with redrive policy pointing to the DLQ
resource "aws_sqs_queue" "main" {
  name                       = var.queue_name
  visibility_timeout_seconds = 300    # 5 min — covers worst-case processing time
  message_retention_seconds  = 345600 # 4 days

  # After 3 failed receive attempts, route to DLQ
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Name        = var.queue_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
