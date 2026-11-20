variable "environment" {
  description = "Entorno de despliegue"
  type        = string
}

variable "data_bucket_name" {
  description = "S3 bucket name for ingested data"
  type        = string
}

variable "data_prefix" {
  description = "Prefix for ingested objects in S3"
  type        = string
  default     = "ingesta"
}

variable "firehose_prefix" {
  description = "S3 prefix exclusivo para los datos entregados por Firehose"
  type        = string
  default     = "ingesta"
}