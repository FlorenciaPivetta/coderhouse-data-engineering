
data "aws_caller_identity" "current" {}

# ============================================================
# IAM ROLE PARA REDSHIFT SERVERLESS
# ============================================================

resource "aws_iam_role" "redshift" {
  name = "redshift-serverless-${var.environment}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "redshift.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# ============================================================
# POLITICA IAM: KINESIS + GLUE + S3 / ICEBERG
# ============================================================

resource "aws_iam_role_policy" "redshift_access" {
  name = "redshift-streaming-glue-s3-access"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Lectura de los streams Kinesis de entrada y salida
      {
        Sid    = "KinesisStreamingRead"
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]

        Resource = [
          var.kinesis_stream_arn,
          var.flink_stateful_output_v2_stream_arn
        ]
      },

      # Listado de streams Kinesis
      # ListStreams requiere Resource = "*"
      {
        Sid      = "KinesisListStreams"
        Effect   = "Allow"
        Action   = ["kinesis:ListStreams"]
        Resource = "*"
      },

      # Lectura de Glue Data Catalog
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"

        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions"
        ]

        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
        ]
      },

      # Listado del bucket historico
      {
        Sid      = "HistoricalIcebergList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.data_bucket_name}"

        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.data_prefix}/*"]
          }
        }
      },

      # Lectura de archivos Iceberg en S3
      {
        Sid      = "HistoricalIcebergRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${var.data_bucket_name}/${var.data_prefix}/*"
      }
    ]
  })
}

# ============================================================
# REDSHIFT SERVERLESS - NAMESPACE
# ============================================================

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name        = "redshift-${var.environment}-namespace"
  db_name               = "analytics"
  iam_roles             = [aws_iam_role.redshift.arn]
  default_iam_role_arn  = aws_iam_role.redshift.arn
  manage_admin_password = true
}

# ============================================================
# SECURITY GROUP DE REDSHIFT
# ============================================================

resource "aws_security_group" "redshift" {
  name        = "redshift-${var.environment}-sg"
  description = "Security group privado para Redshift Serverless"
  vpc_id      = var.vpc_id

  # Sin reglas de ingreso: se utilizara Redshift Data API.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "redshift-${var.environment}-sg"
    Environment = var.environment
  }
}

# ============================================================
# SECURITY GROUP PARA EL ENDPOINT PRIVADO DE KINESIS
# ============================================================

resource "aws_security_group" "kinesis_endpoint" {
  name        = "redshift-${var.environment}-kinesis-endpoint-sg"
  description = "Permite HTTPS desde Redshift hacia Kinesis"
  vpc_id      = var.vpc_id

  ingress {
    description     = "HTTPS desde Redshift"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.redshift.id]
  }

  tags = {
    Name        = "redshift-${var.environment}-kinesis-endpoint-sg"
    Environment = var.environment
  }
}

# ============================================================
# VPC ENDPOINT PRIVADO PARA KINESIS DATA STREAMS
# ============================================================

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.subnet_ids
  security_group_ids  = [aws_security_group.kinesis_endpoint.id]
  private_dns_enabled = true

  # Politica restrictiva del endpoint Kinesis
  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      # Permitir listado de streams al rol de Redshift
      {
        Sid    = "AllowRedshiftListStreams"
        Effect = "Allow"

        Principal = {
          AWS = aws_iam_role.redshift.arn
        }

        Action   = ["kinesis:ListStreams"]
        Resource = "*"
      },

      # Permitir lectura exclusivamente de los streams del proyecto
      {
        Sid    = "AllowRedshiftReadProjectStreams"
        Effect = "Allow"

        Principal = {
          AWS = aws_iam_role.redshift.arn
        }

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]

        Resource = [
          var.kinesis_stream_arn,
          var.flink_stateful_output_v2_stream_arn
        ]
      }
    ]
  })

  tags = {
    Name        = "redshift-${var.environment}-kinesis-endpoint"
    Environment = var.environment
  }
}

# ============================================================
# REDSHIFT SERVERLESS - WORKGROUP
# ============================================================

resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name      = "redshift-${var.environment}-wg"
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  base_capacity       = var.base_capacity
  publicly_accessible = false
  subnet_ids          = var.subnet_ids
  security_group_ids  = [aws_security_group.redshift.id]

  tags = {
    Name        = "redshift-${var.environment}-wg"
    Environment = var.environment
  }
}
