
module "network" {
  source = "../../modules/network"

  providers = {
    aws = aws.network
  }

  aws_region           = "us-east-1"
  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
}
module "identity" {
  source = "../../modules/identity"

  environment = var.environment

  data_service_principals = [
    "lambda.amazonaws.com"
  ]

  data_bucket_name    = var.data_bucket_name
  data_prefix         = var.data_prefix
  audit_principal_arn = var.audit_principal_arn
}

module "kinesis" {
  source = "../../modules/kinesis"

  environment = var.environment
}

module "flink" {
  source = "../../modules/flink"

  environment          = var.environment
  aws_region           = var.aws_region
  data_bucket_name     = var.data_bucket_name
  artifact_bucket_name = "coderhouse-flink-artifacts-662241429835-dev"

  application_name = "preentrega4-flink"

  artifact_path = "${path.root}/../../target/flink-kinesis-preentrega4-1.0.0-managed-flink.zip"

  kinesis_stream_arn = var.kinesis_stream_arn

  checkpoint_interval = 60000
  parallelism         = 1
}