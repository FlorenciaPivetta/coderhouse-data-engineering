
# ---------------------------------------------------------
# IAM: Rol de ejecución para Amazon Managed Flink
# ---------------------------------------------------------

data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink_execution" {
  name               = "${var.environment}-flink-execution-role"
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

# ---------------------------------------------------------
# S3: Bucket, versionado y subida del artefacto ZIP
# ---------------------------------------------------------

resource "aws_s3_bucket" "flink_artifacts" {
  bucket = var.artifact_bucket_name

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

# Habilitar versionado para conservar cada versión del ZIP
resource "aws_s3_bucket_versioning" "flink_artifacts" {
  bucket = aws_s3_bucket.flink_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Subir el artefacto después de habilitar el versionado
resource "aws_s3_object" "flink_artifact" {
  bucket = aws_s3_bucket.flink_artifacts.id
  key    = var.artifact_key
  source = var.artifact_path
  etag   = filemd5(var.artifact_path)

  depends_on = [
    aws_s3_bucket_versioning.flink_artifacts
  ]

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

# ---------------------------------------------------------
# CloudWatch: Grupo y stream de logs
# ---------------------------------------------------------

resource "aws_cloudwatch_log_group" "flink" {
  name              = "/aws/kinesis-analytics/${var.application_name}"
  retention_in_days = 7

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# ---------------------------------------------------------
# IAM: Permisos de Kinesis, S3 y CloudWatch
# ---------------------------------------------------------

data "aws_iam_policy_document" "flink_permissions" {
  statement {
    sid    = "ReadKinesisStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards"
    ]

    resources = [var.kinesis_stream_arn]
  }

  # Permisos para leer el artefacto ZIP y su versión específica
  statement {
    sid    = "ReadApplicationArtifact"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ]

    resources = [
      "${aws_s3_bucket.flink_artifacts.arn}/${var.artifact_key}"
    ]
  }

  statement {
    sid    = "WriteCloudWatchLogs"
    effect = "Allow"

    actions = [
      "logs:DescribeLogStreams",
      "logs:PutLogEvents"
    ]

    resources = [
      "${aws_cloudwatch_log_group.flink.arn}:*"
    ]
  }
}

resource "aws_iam_role_policy" "flink_permissions" {
  name   = "${var.environment}-flink-execution-policy"
  role   = aws_iam_role.flink_execution.id
  policy = data.aws_iam_policy_document.flink_permissions.json
}

# ---------------------------------------------------------
# Managed Service for Apache Flink
# ---------------------------------------------------------

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = var.application_name
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink_execution.arn
  application_mode       = "STREAMING"

  description = "Pre-entrega 4: procesamiento stateful de eventos de Kinesis"

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"

      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.flink_artifacts.arn
          file_key       = aws_s3_object.flink_artifact.key
          object_version = aws_s3_object.flink_artifact.version_id
        }
      }
    }

    environment_properties {
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"


        property_map = {
          python  = "main.py"
          jarfile = "flink/lib/pyflink-dependencies.jar"
          pyFiles = "flink/lib/flink-package.zip"
        }
      }

      property_group {
        property_group_id = "InputStream0"

        property_map = {
          "stream.arn"                 = var.kinesis_stream_arn
          "aws.region"                 = var.aws_region
          "flink.source.init.position" = "TRIM_HORIZON"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval
        min_pause_between_checkpoints = 30000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = false
        parallelism          = var.parallelism
        parallelism_per_kpu  = 1
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }

  depends_on = [
    aws_iam_role_policy.flink_permissions
  ]
}
