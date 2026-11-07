
# ---------------------------------------------------------
# IAM: Rol de ejecución para Amazon Managed Flink
# ---------------------------------------------------------

data "aws_caller_identity" "current" {}

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
# S3: Bucket de artefactos, versionado y ZIP de Flink
# ---------------------------------------------------------

resource "aws_s3_bucket" "flink_artifacts" {
  bucket = var.artifact_bucket_name

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

resource "aws_s3_bucket_versioning" "flink_artifacts" {
  bucket = aws_s3_bucket.flink_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "flink_artifact" {
  bucket      = aws_s3_bucket.flink_artifacts.id
  key         = var.artifact_key
  source      = var.artifact_path
  source_hash = filemd5(var.artifact_path)

  depends_on = [
    aws_s3_bucket_versioning.flink_artifacts
  ]

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

# ---------------------------------------------------------
# S3: Versionado del bucket Lakehouse existente
# ---------------------------------------------------------

resource "aws_s3_bucket_versioning" "lakehouse" {
  provider = aws.lakehouse
  bucket   = var.data_bucket_name

  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------
# AWS Glue: Base de datos del Lakehouse
# ---------------------------------------------------------

resource "aws_glue_catalog_database" "lakehouse" {
  name         = "lakehouse_db"
  description  = "Catalogo de datos Iceberg para el proyecto CoderHouse"
  location_uri = "s3://${var.data_bucket_name}/${var.data_prefix}/"
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
# IAM: Permisos de ejecución para Flink
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

  # Acceso al catalogo AWS Glue para Iceberg
  statement {
    sid    = "AccessGlueCatalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable"
    ]

    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/lakehouse_db",
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/lakehouse_db/*"
    ]
  }

  # Consulta de ubicacion del bucket Lakehouse
  statement {
    sid    = "GetLakehouseBucketLocation"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation"
    ]

    resources = [
      "arn:aws:s3:::${var.data_bucket_name}"
    ]
  }

  # Listado limitado al prefijo del Lakehouse
  statement {
    sid    = "ListLakehouseBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      "arn:aws:s3:::${var.data_bucket_name}"
    ]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"

      values = [
        var.data_prefix,
        "${var.data_prefix}/*"
      ]
    }
  }

  # Lectura, escritura y administracion de archivos Iceberg
  statement {
    sid    = "WriteLakehouseObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "arn:aws:s3:::${var.data_bucket_name}/${var.data_prefix}/*"
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

      # Configuracion de ejecucion PyFlink
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"

        property_map = {
          python  = "main.py"
          jarfile = "flink/lib/pyflink-dependencies.jar"
          pyArchives = "flink/lib/flink-package.zip"
       }
      }

      # Configuracion del origen Kinesis
      property_group {
        property_group_id = "InputStream0"

        property_map = {
          "stream.arn"                 = var.kinesis_stream_arn
          "aws.region"                 = var.aws_region
          "flink.source.init.position" = "TRIM_HORIZON"
        }
      }

      # Configuracion del catalogo Iceberg
      property_group {
        property_group_id = "Iceberg"

        property_map = {
          "catalog.name"     = "glue_catalog"
          "catalog.database" = "lakehouse_db"
          "catalog.region"   = var.aws_region
          "warehouse"        = "s3://${var.data_bucket_name}/${var.data_prefix}/"
          "s3.region"        = "us-east-1"
          "table.name"       = "event_window_aggregations"
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
    aws_iam_role_policy.flink_permissions,
    aws_glue_catalog_database.lakehouse,
    aws_s3_bucket_versioning.lakehouse
  ]
}
