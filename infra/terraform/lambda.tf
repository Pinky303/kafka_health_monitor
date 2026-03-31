resource "aws_iam_role" "lambda_exec" {
  name = "health-monitor-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_sns_ses_sqs" {
  name = "lambda-sns-ses-sqs-policy"
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sns:Publish"], Resource = "*" },
      { Effect = "Allow", Action = ["ses:SendEmail"], Resource = "*" },
      { Effect = "Allow", Action = ["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes"], Resource = aws_sqs_queue.alerts.arn },
    ]
  })
}

resource "aws_lambda_function" "alert_handler" {
  function_name = "patient-alert-handler"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "alert_handler.lambda_handler"
  runtime       = "python3.11"
  filename      = "${path.module}/../../lambda/alert_handler.zip"
  timeout       = 30
  memory_size   = 128

  environment {
    variables = {
      DOCTOR_PHONE       = var.doctor_phone
      DOCTOR_EMAIL       = var.doctor_email
      SNS_ALERT_TOPIC_ARN = aws_sns_topic.alerts.arn
      HOSPITAL_NAME      = var.hospital_name
      SENDER_EMAIL       = "alerts@hospital.com"
    }
  }
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.alerts.arn
  function_name    = aws_lambda_function.alert_handler.arn
  batch_size       = 5
  enabled          = true
}

output "lambda_function_name" { value = aws_lambda_function.alert_handler.function_name }
