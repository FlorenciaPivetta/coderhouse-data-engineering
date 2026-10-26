
variable "environment" {
  description = "Environment name"
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
