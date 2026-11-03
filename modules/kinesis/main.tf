data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.environment}-firehose-delivery-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json

  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
