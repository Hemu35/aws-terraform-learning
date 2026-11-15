# ============================================================
# Remote State Backend Configuration (Dev)
# ============================================================
# WHY Remote State in S3 with DynamoDB Locking?
#   1. Team Collaboration: Storing state locally (terraform.tfstate)
#      leads to state drift, accidental overwrites, and conflicts.
#   2. State Locking: DynamoDB prevents concurrent applies from
#      corrupting state.
#   3. Security & Durability: S3 provides 11 9's durability and
#      enables encryption at rest + versioning for rollback.
#
# INSTRUCTIONS:
#   Uncomment this block AFTER creating the backend S3 bucket and
#   DynamoDB lock table (e.g. via AWS Console or a bootstrap script).
#   Then run `terraform init` to migrate state.
#
# terraform {
#   backend "s3" {
#     bucket         = "doc-processor-terraform-state-dev"
#     key            = "dev/terraform.tfstate"
#     region         = "us-east-1"
#     encrypt        = true
#     dynamodb_table = "doc-processor-terraform-locks-dev"
#   }
# }
