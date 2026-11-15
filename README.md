# AWS Document Processing API

A local-first document processing API built with FastAPI, backed by AWS services (S3, DynamoDB, SQS), running entirely on **LocalStack** — no real AWS account needed for development.

> **Interview Project** — Demonstrates AWS Solutions Architect patterns: async job queue, event-driven worker, presigned URLs, and IaC via Terraform.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LOCAL DEVELOPMENT                            │
│                                                                     │
│   ┌──────────┐     ┌──────────────┐     ┌──────────────────────┐   │
│   │  Client  │────▶│   FastAPI    │────▶│   S3 (documents)     │   │
│   │(HTTP/S3) │     │  :8000       │     │   Upload input file  │   │
│   └──────────┘     └──────┬───────┘     └──────────────────────┘   │
│                           │                                         │
│                           ▼                                         │
│                   ┌──────────────┐     ┌──────────────────────┐    │
│                   │  DynamoDB    │     │   SQS (jobs-queue)   │    │
│                   │  jobs table  │◀────│   Enqueue job_id     │    │
│                   │  PENDING→... │     └──────────┬───────────┘    │
│                   └──────────────┘                │                 │
│                                                   ▼                 │
│                                         ┌──────────────────────┐   │
│                                         │   Worker Process     │   │
│                                         │   Long-poll SQS      │   │
│                                         │   Process document   │   │
│                                         │   Write result→S3    │   │
│                                         │   Update DynamoDB    │   │
│                                         └──────────────────────┘   │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  LocalStack  :4566  (emulates S3 + DynamoDB + SQS)         │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## System Flow

```
Client                FastAPI               AWS Services          Worker
  │                     │                       │                   │
  │  POST /jobs/submit  │                       │                   │
  │  (multipart file)   │                       │                   │
  │────────────────────▶│                       │                   │
  │                     │── upload file ───────▶│ S3 (input key)   │
  │                     │── create job ────────▶│ DynamoDB PENDING  │
  │                     │── send message ──────▶│ SQS job_id       │
  │◀────────────────────│                       │                   │
  │  202 { job_id }     │                       │                   │
  │                     │                       │◀─── poll msgs ───│
  │                     │                       │───── job_id ─────▶│
  │                     │                       │                   │── download from S3
  │                     │                       │                   │── process doc
  │                     │                       │◀── upload result ─│ S3 (output key)
  │                     │                       │◀── update status ─│ DynamoDB COMPLETED
  │                     │                       │◀── delete msg ────│ SQS ack
  │                     │                       │                   │
  │  GET /jobs/{id}     │                       │                   │
  │────────────────────▶│                       │                   │
  │                     │── get job ───────────▶│ DynamoDB          │
  │◀────────────────────│                       │                   │
  │  200 { status, url }│                       │                   │
```

---

## Local Dev Quickstart

### Prerequisites
- [Docker + Docker Compose](https://docs.docker.com/get-docker/)
- Python 3.12+
- [Terraform](https://www.terraform.io/downloads) (optional, for infra review)

### 1. Clone & configure

```bash
git clone <repo>
cd aws-terraform-learning
cp .env.example .env
```

### 2. Start LocalStack + API

```bash
docker-compose up --build
```

This starts:
- **LocalStack** on `http://localhost:4566` (S3, DynamoDB, SQS)
- **FastAPI** on `http://localhost:8000`
- **Worker** (background SQS consumer)

### 3. Bootstrap AWS resources in LocalStack

```bash
# Run once after LocalStack is healthy
docker-compose exec api python scripts/bootstrap_local.py
```

### 4. Test the API

```bash
# Submit a document
curl -X POST http://localhost:8000/api/v1/jobs \
  -F "file=@/path/to/document.pdf"

# Check status
curl http://localhost:8000/api/v1/jobs/{job_id}

# Health check
curl http://localhost:8000/health
```

### 5. Install Python deps (for local dev without Docker)

```bash
pip install -e ".[dev]"
uvicorn app.main:app --reload
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | `test` | AWS key (use `test` for LocalStack) |
| `AWS_SECRET_ACCESS_KEY` | `test` | AWS secret (use `test` for LocalStack) |
| `AWS_ENDPOINT_URL` | `http://localhost:4566` | LocalStack endpoint; unset for real AWS |
| `DYNAMODB_TABLE_NAME` | `jobs` | DynamoDB table name |
| `S3_BUCKET_NAME` | `documents` | S3 bucket name |
| `SQS_QUEUE_NAME` | `jobs-queue` | SQS queue name |
| `SQS_QUEUE_URL` | `http://localhost:4566/000000000000/jobs-queue` | Full SQS URL |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `ENVIRONMENT` | `local` | `local` or `production` |

> **Never commit `.env`** — only `.env.example` belongs in source control.

---

## API Endpoints

### `POST /api/v1/jobs`
Upload a document for processing.

**Request:** `multipart/form-data` with field `file` (and optional `filename`)

**Response `202 Accepted`:**
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "PENDING",
  "message": "Job submitted successfully"
}
```

---

### `GET /api/v1/jobs/{job_id}`
Get job status and result URL.

**Response `200 OK`:**
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "COMPLETED",
  "filename": "document.pdf",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:01:00Z",
  "result_url": "https://...",
  "error_message": null
}
```

**Response `404 Not Found`:** Job not found.

---

### `GET /health`
Liveness check.

**Response `200 OK`:**
```json
{ "status": "ok", "version": "0.1.0" }
```

---

## Terraform (Plan Only)

The `terraform/` directory contains IaC for the production AWS resources. During local development, **only `plan` is used** — never `apply` against a real account.

```bash
cd terraform/environments/dev
terraform init
terraform plan -var-file="terraform.tfvars.example"
```

> `terraform apply` is intentionally excluded from local workflow to prevent accidental provisioning of real AWS resources.

---

## Project Structure

```
aws-terraform-learning/
├── app/
│   ├── api/            # FastAPI routers
│   ├── core/           # Config, logging, exceptions
│   ├── models/         # Pydantic models
│   ├── services/       # S3, SQS, DynamoDB clients
│   └── worker/         # SQS consumer / processor
├── tests/
│   └── unit/
├── terraform/          # IaC (plan only locally)
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── README.md
```
