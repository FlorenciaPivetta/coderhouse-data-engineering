
resource "aws_kinesis_stream" "this" {
  name             = "${var.environment}-data-stream"
  shard_count      = 2
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Project     = "coderhouse-data-engineering"
  }
}
