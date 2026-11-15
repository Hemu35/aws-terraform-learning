# ============================================================
# DynamoDB Module — Job State Store
# ============================================================
# WHY DynamoDB?
#   Single-digit millisecond reads/writes at any scale with zero
#   operational overhead. For job status tracking (key-value
#   pattern: job_id → status), DynamoDB is the natural fit on AWS.
#   Interviewer signal: choosing DynamoDB over RDS shows you
#   understand workload characteristics, not just "use Postgres".
#
# WHY PAY_PER_REQUEST (on-demand)?
#   In dev/learning environments traffic is bursty and low volume.
#   On-demand billing charges per request rather than for reserved
#   capacity. For predictable high-volume prod workloads, provisioned
#   throughput would be cheaper — but for a dev environment this
#   avoids paying for idle capacity overnight.
#
# WHY TTL (expires_at)?
#   Completed job records don't need to live forever. TTL lets
#   DynamoDB delete stale items automatically at no extra cost,
#   keeping the table lean and demonstrating housekeeping awareness.
#
# WHY PITR disabled in dev?
#   Point-in-time recovery costs ~20% extra on storage. It's
#   production-critical but overkill (and unnecessarily expensive)
#   for a dev/learning environment.
# ============================================================

resource "aws_dynamodb_table" "jobs" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST" # On-demand — no capacity planning needed in dev

  hash_key = "job_id" # Simple primary key — job_id is always known at access time

  attribute {
    name = "job_id"
    type = "S" # String
  }

  # TTL — items with expires_at set are deleted automatically (free housekeeping)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  # PITR disabled in dev to avoid ~20% storage cost premium
  point_in_time_recovery {
    enabled = false
  }

  tags = {
    Name        = var.table_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
