
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
  bucket         = "coderhouse-data-tfstate-662241429835-us-east-1"
  key            = "coderhouse-data-engineering/dev/terraform.tfstate"
  region         = "us-east-1"
  encrypt        = true
  dynamodb_table = "terraform-state-lock"
}
} 

provider "aws" {
  region = var.aws_region
}
