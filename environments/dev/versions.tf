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

# Región principal: Flink, Kinesis y Glue
provider "aws" {
  region = "us-east-2"
}

# Región de infraestructura de red
provider "aws" {
  alias  = "network"
  region = "us-east-1"
}

# Región del bucket S3 del Lakehouse
provider "aws" {
  alias  = "lakehouse"
  region = "us-east-1"
}
