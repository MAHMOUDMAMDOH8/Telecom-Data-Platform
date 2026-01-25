#!/bin/bash
set -e

echo "[INFO] Running LocalStack init script..."

AWS="aws --endpoint-url=http://localhost:4566 --region us-east-1 \
     --access-key-id test --secret-access-key test"

# Create bucket if it does not exist
if ! $AWS s3api head-bucket --bucket telecom_lakehouse 2>/dev/null; then
  echo "[INFO] Creating bucket telecom_lakehouse"
  $AWS s3api create-bucket --bucket telecom_lakehouse --region us-east-1
else
  echo "[INFO] Bucket telecom_lakehouse already exists"
fi

# Bronze
$AWS s3api put-object --bucket telecom_lakehouse --key "bronze_layer/batch_job/"
$AWS s3api put-object --bucket telecom_lakehouse --key "bronze_layer/stream_job/"

# Silver
$AWS s3api put-object --bucket telecom_lakehouse --key "silver_layer/processing_data/"
$AWS s3api put-object --bucket telecom_lakehouse --key "silver_layer/valid_data/"
$AWS s3api put-object --bucket telecom_lakehouse --key "silver_layer/reject_data/"


echo "[INFO] Init script completed ✅"
