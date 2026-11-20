
# =========================================================
# NETWORK EXISTENTE - us-east-1
# =========================================================

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

# =========================================================
# IDENTITY
# =========================================================

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

# =========================================================
# KINESIS
# =========================================================

module "kinesis" {
  source = "../../modules/kinesis"

  environment      = var.environment
  data_bucket_name = var.data_bucket_name
  data_prefix      = var.data_prefix
  firehose_prefix  = "ingesta"
}

# =========================================================
# FLINK
# =========================================================

module "flink" {
  source = "../../modules/flink"

  providers = {
    aws           = aws
    aws.lakehouse = aws.lakehouse
  }

  environment          = var.environment
  aws_region           = var.aws_region
  data_bucket_name     = var.data_bucket_name
  data_prefix          = var.data_prefix
  artifact_bucket_name = "coderhouse-flink-artifacts-662241429835-dev"

  application_name = "preentrega4-flink"

  artifact_path = "${path.root}/../../target/flink-kinesis-preentrega4-1.0.0-managed-flink.zip"

  # Stream de entrada administrado por el módulo Kinesis
  kinesis_stream_arn = module.kinesis.input_stream_arn

  kinesis_output_stream_arn  = module.kinesis.flink_stateful_output_v2_stream_arn
  kinesis_output_stream_name = module.kinesis.flink_stateful_output_v2_stream_name

  checkpoint_interval = 60000
  parallelism         = 1
}

# =========================================================
# REDSHIFT - CONSULTA DINAMICA DE RED EXISTENTE - us-east-2
# =========================================================

data "aws_vpc" "redshift_default" {
  default = true
}

data "aws_subnets" "redshift_default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.redshift_default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# =========================================================
# REDSHIFT
# =========================================================

module "redshift" {
  source = "../../modules/redshift"

  environment = var.environment
  aws_region  = var.aws_region

  vpc_id = data.aws_vpc.redshift_default.id

  subnet_ids = data.aws_subnets.redshift_default.ids

  # Stream de entrada administrado por el módulo Kinesis
  kinesis_stream_arn = module.kinesis.input_stream_arn

  flink_stateful_output_v2_stream_arn = module.kinesis.flink_stateful_output_v2_stream_arn

  data_bucket_name = var.data_bucket_name
  data_prefix      = var.data_prefix
}
