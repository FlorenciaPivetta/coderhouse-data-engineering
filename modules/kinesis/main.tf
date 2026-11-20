# =========================================================
# KINESIS MODULE
# =========================================================

# ---------------------------------------------------------
# 1. Stream de entrada existente
# Se conserva la dirección del recurso en Terraform State
# ---------------------------------------------------------

resource "aws_kinesis_stream" "input" {
  name             = "${var.environment}-data-stream"
  retention_period = 24
  encryption_type  = "NONE"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# ---------------------------------------------------------
# 2. Stream de salida de Flink existente
# Se conserva la dirección y configuración actuales
# ---------------------------------------------------------

resource "aws_kinesis_stream" "flink_stateful_output_v2" {
  name             = "${var.environment}-flink-stateful-output-v2"
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Project     = "CoderHouse-DataEngineering"
    Purpose     = "Flink Stateful Aggregation Output - Clean Run"
  }
}