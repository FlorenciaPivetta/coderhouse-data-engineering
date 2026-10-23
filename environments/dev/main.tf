
module "network" {
  source = "../../modules/network"

  aws_region           = var.aws_region
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

  environment      = var.environment
  data_bucket_name = var.data_bucket_name
  data_prefix      = var.data_prefix
}
