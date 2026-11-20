
data "aws_iam_policy_document" "data_service_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = var.data_service_principals
    }
  }
}

resource "aws_iam_role" "data_service" {
  name               = "${var.environment}-data-service-role"
  assume_role_policy = data.aws_iam_policy_document.data_service_assume_role.json

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

data "aws_iam_policy_document" "s3_data_access" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.data_bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [var.data_prefix, "${var.data_prefix}/*"]
    }
  }

  statement {
    sid       = "ReadWriteObjectsInPrefix"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["arn:aws:s3:::${var.data_bucket_name}/${var.data_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "data_service_s3" {
  name   = "${var.environment}-data-service-s3"
  role   = aws_iam_role.data_service.id
  policy = data.aws_iam_policy_document.s3_data_access.json
}

data "aws_iam_policy_document" "audit_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [var.audit_principal_arn]
    }
  }
}

resource "aws_iam_role" "audit_readonly" {
  name               = "${var.environment}-audit-readonly-role"
  assume_role_policy = data.aws_iam_policy_document.audit_assume_role.json

  tags = {
    Environment = var.environment
    Project     = "CoderHouse-DataEngineering"
  }
}

data "aws_iam_policy_document" "audit_readonly_permissions" {
  statement {
    sid    = "DescribeEc2Infrastructure"
    effect = "Allow"

    actions = [
      "ec2:DescribeAvailabilityZones",
      "ec2:DescribeInstances",
      "ec2:DescribeNetworkAcls",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeRouteTables",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeVpcs"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ReadIamRoleMetadata"
    effect = "Allow"

    actions = [
      "iam:GetRole",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:GetRolePolicy"
    ]

    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.environment}-*"
    ]
  }

  statement {
    sid    = "ReadDataBucketMetadata"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:GetBucketVersioning",
      "s3:ListBucket"
    ]

    resources = [
      "arn:aws:s3:::${var.data_bucket_name}"
    ]
  }

  statement {
    sid    = "ReadObjectsInDataPrefix"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "arn:aws:s3:::${var.data_bucket_name}/${var.data_prefix}/*"
    ]
  }
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role_policy" "audit_readonly" {
  name   = "${var.environment}-audit-readonly-policy"
  role   = aws_iam_role.audit_readonly.id
  policy = data.aws_iam_policy_document.audit_readonly_permissions.json
}
