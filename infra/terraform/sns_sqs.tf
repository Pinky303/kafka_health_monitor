resource "aws_sns_topic" "alerts" {
  name = "patient-critical-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.doctor_email
}

resource "aws_sqs_queue" "alerts" {
  name                       = "patient-alerts-queue"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
}

resource "aws_sqs_queue" "alerts_dlq" {
  name                      = "patient-alerts-dlq"
  message_retention_seconds = 604800  # 7 days
}

resource "aws_sqs_queue_redrive_policy" "alerts" {
  queue_url = aws_sqs_queue.alerts.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.alerts_dlq.arn
    maxReceiveCount     = 3
  })
}

output "sns_topic_arn"   { value = aws_sns_topic.alerts.arn }
output "sqs_queue_url"   { value = aws_sqs_queue.alerts.url }
