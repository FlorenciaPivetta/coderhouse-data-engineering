
Warning: Deprecated Parameter

The parameter "dynamodb_table" is deprecated. Use parameter "use_lockfile" instead.
Acquiring state lock. This may take a few moments...
module.identity.data.aws_iam_policy_document.data_service_assume_role: Reading...
module.identity.data.aws_iam_policy_document.audit_readonly_permissions: Reading...
module.network.data.aws_availability_zones.available: Reading...
module.identity.data.aws_iam_policy_document.audit_assume_role: Reading...
module.identity.data.aws_iam_policy_document.s3_data_access: Reading...
module.network.aws_vpc.main: Refreshing state... [id=vpc-0ef4323533495f838]
module.identity.data.aws_iam_policy_document.audit_assume_role: Read complete after 0s [id=1424080243]
module.identity.data.aws_iam_policy_document.data_service_assume_role: Read complete after 0s [id=2690255455]
module.identity.data.aws_iam_policy_document.audit_readonly_permissions: Read complete after 0s [id=1118454466]
module.identity.data.aws_iam_policy_document.s3_data_access: Read complete after 0s [id=4142395191]
module.identity.aws_iam_role.audit_readonly: Refreshing state... [id=dev-audit-readonly-role]
module.identity.aws_iam_role.data_service: Refreshing state... [id=dev-data-service-role]
module.network.data.aws_availability_zones.available: Read complete after 0s [id=us-east-1]
module.identity.aws_iam_role_policy.data_service_s3: Refreshing state... [id=dev-data-service-role:dev-data-service-s3]
module.identity.aws_iam_role_policy.audit_readonly: Refreshing state... [id=dev-audit-readonly-role:dev-audit-readonly-policy]
module.network.aws_route_table.private: Refreshing state... [id=rtb-07b3e1e1942423ddd]
module.network.aws_subnet.private[1]: Refreshing state... [id=subnet-099e57b87fc514b8d]
module.network.aws_subnet.private[0]: Refreshing state... [id=subnet-071890b0fcd5e57d5]
module.network.aws_vpc_endpoint.s3: Refreshing state... [id=vpce-0e85392eb0386c779]
module.network.aws_route_table_association.private[0]: Refreshing state... [id=rtbassoc-0ee7a9cd03e77632d]
module.network.aws_route_table_association.private[1]: Refreshing state... [id=rtbassoc-088fad7a3c240ce7a]

No changes. Your infrastructure matches the configuration.

Terraform has compared your real infrastructure against your configuration
and found no differences, so no changes are needed.
