
variable "aws_region" {
  description = "Región de AWS donde se desplegará la infraestructura"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Nombre del entorno de despliegue"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "Bloque CIDR de la VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Bloques CIDR para las dos subredes privadas"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "data_bucket_name" {
  description = "Nombre del bucket S3 destinado a los datos"
  type        = string
  default     = "coderhouse-data-flor-dev"
}

variable "data_prefix" {
  description = "Prefijo de S3 al que accederá el servicio"
  type        = string
  default     = "lakehouse"
}

variable "audit_principal_arn" {
  description = "ARN del principal autorizado a asumir el rol de auditoría"
  type        = string
}

