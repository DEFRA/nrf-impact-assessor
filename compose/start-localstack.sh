#!/bin/bash
export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Dead-letter queue for jobs that fail maxReceiveCount times
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name nrf-impact-assessment-jobs-dlq

DLQ_ARN=$(aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/nrf-impact-assessment-jobs-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Main SQS queue for assessment jobs, wired to the DLQ after 5 receives
aws --endpoint-url=http://localhost:4566 sqs create-queue \
  --queue-name nrf-impact-assessment-jobs \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

# SNS topic (mirrors nrf-backend's topic)
aws --endpoint-url=http://localhost:4566 sns create-topic --name nrf-quote-estimate-request

# Subscribe SQS queue to SNS topic (delivers SNS envelope to SQS)
aws --endpoint-url=http://localhost:4566 sns subscribe \
  --topic-arn arn:aws:sns:eu-west-2:000000000000:nrf-quote-estimate-request \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:eu-west-2:000000000000:nrf-impact-assessment-jobs

echo "LocalStack setup complete: SQS queue + DLQ, SNS topic, SNS→SQS subscription"
