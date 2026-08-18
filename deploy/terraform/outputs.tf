output "kafka_bootstrap_brokers" {
  description = "Plaintext bootstrap servers, for TAPELINE_KAFKA_BROKERS."
  value       = aws_msk_cluster.main.bootstrap_brokers
}

output "kafka_bootstrap_brokers_tls" {
  description = "TLS bootstrap servers."
  value       = aws_msk_cluster.main.bootstrap_brokers_tls
}

output "eks_cluster_name" {
  value = aws_eks_cluster.main.name
}

output "eks_cluster_endpoint" {
  value = aws_eks_cluster.main.endpoint
}

output "lakehouse_warehouse_uri" {
  description = "For TAPELINE_ICEBERG_WAREHOUSE."
  value       = "s3://${aws_s3_bucket.lakehouse.id}/warehouse"
}

output "flink_role_arn" {
  description = "Annotate the flink service account with this for IRSA."
  value       = aws_iam_role.flink_lakehouse.arn
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${aws_eks_cluster.main.name}"
}
