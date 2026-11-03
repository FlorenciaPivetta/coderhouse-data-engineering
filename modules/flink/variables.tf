variable "environment" {
  description = "Entorno de despliegue"
  type        = string
}

variable "aws_region" {
  description = "Región de AWS"
  type        = string
}

variable "data_bucket_name" {
  description = "Bucket S3 para el artefacto de Flink"
  type        = string
}

variable "application_name" {
  description = "Nombre de la aplicación Managed Flink"
  type        = string
  default     = "preentrega4-flink"
}

variable "artifact_path" {
  description = "Ruta local del ZIP de la aplicación"
  type        = string
}

variable "artifact_key" {
  description = "Ruta del ZIP dentro del bucket S3"
  type        = string
  default     = "flink/artifacts/flink-kinesis-preentrega4-1.0.0-managed-flink.zip"
}

variable "kinesis_stream_arn" {
  description = "ARN del stream Kinesis de entrada"
  type        = string
}

variable "checkpoint_interval" {
  description = "Intervalo de checkpoints en milisegundos"
  type        = number
  default     = 60000
}

variable "parallelism" {
  description = "Paralelismo de la aplicación Flink"
  type        = number
  default     = 1
}

variable "artifact_bucket_name" {
  description = "Nombre del bucket S3 para almacenar el artefacto de Flink"
  type        = string
}
