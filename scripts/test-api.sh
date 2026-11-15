#!/bin/bash
# test-api.sh
# Demonstrates the Document Processing API.
# Requires the stack to be running: docker-compose up

set -e

BASE_URL="${API_URL:-http://localhost:8000}"

echo "=== Health Check ==="
curl -s "$BASE_URL/health" | python3 -m json.tool
echo

echo "=== Submit a Job ==="
# Create a sample file if none provided
SAMPLE_FILE="${1:-/tmp/test.txt}"
if [ ! -f "$SAMPLE_FILE" ]; then
  echo "Hello from document processor! Word count test." > "$SAMPLE_FILE"
  echo "[test] Created sample file: $SAMPLE_FILE"
fi

SUBMIT_RESPONSE=$(curl -s -X POST "$BASE_URL/api/v1/jobs" \
  -F "file=@$SAMPLE_FILE")

echo "$SUBMIT_RESPONSE" | python3 -m json.tool
JOB_ID=$(echo "$SUBMIT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
echo
echo "Job ID: $JOB_ID"

echo
echo "=== Poll Job Status ==="
for i in 1 2 3 4 5; do
  echo "--- Attempt $i ---"
  curl -s "$BASE_URL/api/v1/jobs/$JOB_ID" | python3 -m json.tool
  sleep 3
done

echo
echo "=== Direct Status Check (replace JOB_ID with your value) ==="
echo "curl $BASE_URL/api/v1/jobs/JOB_ID"
