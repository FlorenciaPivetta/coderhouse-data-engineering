
output "data_service_role_arn" {
  description = "ARN del rol de procesamiento de datos"
  value       = aws_iam_role.data_service.arn
}

output "audit_readonly_role_arn" {
  description = "ARN del rol de auditoria"
  value       = aws_iam_role.audit_readonly.arn
}
