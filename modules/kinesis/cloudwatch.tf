
resource "aws_cloudwatch_metric_alarm" "read_throughput" {
  alarm_name          = "${var.environment}-kinesis-read-throttle"
  alarm_description   = "Kinesis stream read throughput exceeded"
  namespace           = "AWS/Kinesis"
  metric_name         = "ReadProvisionedThroughputExceeded"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.this.name
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_cloudwatch_metric_alarm" "write_throughput" {
  alarm_name          = "${var.environment}-kinesis-write-throttle"
  alarm_description   = "Kinesis stream write throughput exceeded"
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.this.name
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
