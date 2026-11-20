
# =========================================================
# CLOUDWATCH - KINESIS MONITORING
# =========================================================

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name        = "${var.environment}-kinesis-iterator-age"
  alarm_description = "Kinesis consumer is falling behind the stream"

  namespace   = "AWS/Kinesis"
  metric_name = "GetRecords.IteratorAgeMilliseconds"

  statistic = "Maximum"
  period    = 60

  evaluation_periods  = 2
  datapoints_to_alarm = 2

  threshold           = 300000
  comparison_operator = "GreaterThanThreshold"

  treat_missing_data = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.input.name
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Project     = "coderhouse-data-engineering"
  }
}
