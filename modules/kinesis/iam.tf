
# =========================================================
# IAM - KINESIS FIREHOSE DELIVERY
# =========================================================

# ---------------------------------------------------------
# 1. IAM Role
# ---------------------------------------------------------

resource "aws_iam_role" "firehose" {
  name = "${var.environment}-firehose-delivery-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"

        Principal = {
          Service = "firehose.amazonaws.com"
        }

        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
    Project     = "coderhouse-data-engineering"
  }
}

# ---------------------------------------------------------
# 2. IAM Policy
# ---------------------------------------------------------

resource "aws_iam_role_policy" "firehose" {
  name = "${var.environment}-firehose-delivery-policy"
  role = aws_iam_role.firehose.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [

      # ---------------------------------------------------
      # Kinesis: lectura del stream de entrada
      # ---------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]

        Resource = aws_kinesis_stream.input.arn
      },

      # ---------------------------------------------------
      # S3: acceso a metadatos del bucket
      # ---------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads"
        ]

        Resource = "arn:aws:s3:::${var.data_bucket_name}"
      },

      # ---------------------------------------------------
      # S3: operaciones multipart sobre objetos Firehose
      # ---------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "s3:AbortMultipartUpload"
        ]

        Resource = [
          "arn:aws:s3:::${var.data_bucket_name}/${var.firehose_prefix}/*",
          "arn:aws:s3:::${var.data_bucket_name}/${var.firehose_prefix}-errors/*"
        ]
      },

      # ---------------------------------------------------
      # S3: escritura exclusiva en prefijos Firehose
      # ---------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "s3:PutObject"
        ]

        Resource = [
          "arn:aws:s3:::${var.data_bucket_name}/${var.firehose_prefix}/*",
          "arn:aws:s3:::${var.data_bucket_name}/${var.firehose_prefix}-errors/*"
        ]
      },

      # ---------------------------------------------------
      # CloudWatch Logs
      # ---------------------------------------------------

      {
        Effect = "Allow"

        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]

        Resource = "${aws_cloudwatch_log_group.firehose.arn}:*"
      }
    ]
  })
}
