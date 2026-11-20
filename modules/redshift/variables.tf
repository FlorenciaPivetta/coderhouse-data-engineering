
variable "environment" {
  description = "Entorno de despliegue"
  type        = string
}

variable "aws_region" {
  description = "Region de AWS"
  type        = string
}

variable "vpc_id" {
  description = "VPC existente donde se desplegara Redshift"
  type        = string
}

variable "subnet_ids" {
  description = "Subredes disponibles para Redshift"
  type        = list(string)
}

variable "kinesis_stream_arn" {
  description = "ARN del stream Kinesis de entrada"
  type        = string
}


variable "glue_database_name" {
  description = "Base de datos Glue del Lakehouse"
  type        = string
  default     = "lakehouse_db"
}

variable "base_capacity" {
  description = "Capacidad inicial de Redshift Serverless en RPU"
  type        = number
  default     = 4
}

variable "data_bucket_name" {
  description = "Bucket S3 que almacena los datos historicos"
  type        = string
}

variable "data_prefix" {
  description = "Prefijo S3 de los datos historicos"
  type        = string
}

variable "flink_stateful_output_v2_stream_arn" {
  description = "ARN del stream Kinesis de salida de Flink"
  type        = string
}