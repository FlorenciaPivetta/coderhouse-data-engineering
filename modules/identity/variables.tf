
variable "environment" {
  description = "Nombre del entorno de despliegue"
  type        = string

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "El entorno debe ser dev, test o prod."
  }
}

variable "data_service_principals" {
  description = "Servicios AWS autorizados a asumir el rol de procesamiento"
  type        = list(string)

  default = ["lambda.amazonaws.com"]

  validation {
    condition     = length(var.data_service_principals) > 0
    error_message = "Debe existir al menos un principal de servicio."
  }
}

variable "data_bucket_name" {
  description = "Nombre del bucket S3 destinado a los datos"
  type        = string

  validation {
    condition     = length(var.data_bucket_name) >= 3 && length(var.data_bucket_name) <= 63
    error_message = "El nombre del bucket debe tener entre 3 y 63 caracteres."
  }
}

variable "data_prefix" {
  description = "Prefijo de S3 al que accederá el servicio"
  type        = string
  default     = "lakehouse"
}

variable "audit_principal_arn" {
  description = "ARN del usuario o rol autorizado a asumir el rol de auditoría"
  type        = string

  validation {
    condition     = can(regex("^arn:[^:]+:iam::[0-9]{12}:(root|user/.+|role/.+)$", var.audit_principal_arn))
    error_message = "Debe especificarse un ARN válido de root, usuario o rol IAM."
  }
}
