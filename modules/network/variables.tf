
variable "aws_region" {
  description = "Region de AWS"
  type        = string
}

variable "environment" {
  description = "Nombre del entorno"
  type        = string
}

variable "vpc_cidr" {
  description = "Bloque CIDR de la VPC"
  type        = string
}

variable "private_subnet_cidrs" {
  description = "Bloques CIDR de las subredes privadas"
  type        = list(string)
}
