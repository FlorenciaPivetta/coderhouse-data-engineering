
output "vpc_id" {
  description = "ID de la VPC del entorno dev"
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "IDs de las subredes privadas"
  value       = module.network.private_subnet_ids
}

output "s3_gateway_endpoint_id" {
  description = "ID del Gateway Endpoint de S3"
  value       = module.network.s3_gateway_endpoint_id
}

output "data_service_role_arn" {
  description = "ARN del rol IAM para el servicio de datos"
  value       = module.identity.data_service_role_arn
}

output "audit_readonly_role_arn" {
  description = "ARN del rol IAM de auditoría"
  value       = module.identity.audit_readonly_role_arn
}