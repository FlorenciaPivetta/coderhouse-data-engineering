
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.environment}-bronze-firehose"
  retention_in_days = 7

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = "${var.environment}-bronze-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.this.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = "arn:aws:s3:::${var.data_bucket_name}"
    prefix              = "${var.data_prefix}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "${var.data_prefix}-errors/!{firehose:error-output-type}/"

    buffering_size     = 5
    buffering_interval = 60
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }

  depends_on = [
    aws_iam_role_policy.firehose
  ]

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

output "firehose_name" {
  description = "Name of the Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.bronze.name
}
