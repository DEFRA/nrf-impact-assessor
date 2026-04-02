#!/bin/bash
export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# S3 bucket for geometry uploads
aws --endpoint-url=http://localhost:4566 s3 mb s3://nrf-impact-input

# SQS queue for assessment jobs
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name nrf-impact-assessment-jobs

# SNS topic (mirrors nrf-backend's topic)
aws --endpoint-url=http://localhost:4566 sns create-topic --name nrf-quote-estimate-request

# Subscribe SQS queue to SNS topic (delivers SNS envelope to SQS)
aws --endpoint-url=http://localhost:4566 sns subscribe \
  --topic-arn arn:aws:sns:eu-west-2:000000000000:nrf-quote-estimate-request \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:eu-west-2:000000000000:nrf-impact-assessment-jobs

echo "LocalStack setup complete: S3 bucket, SQS queue, SNS topic, SNS→SQS subscription"
