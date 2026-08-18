variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "vpc_cidr" {
  description = "CIDR for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zone_count" {
  description = <<-EOT
    Number of AZs to span.

    Three, not two, and the reason is Kafka rather than general good practice:
    a three-broker cluster with replication factor 3 and min.insync.replicas 2
    survives losing one AZ and keeps accepting writes. With two AZs, losing
    one takes the ISR below the minimum and the cluster stops accepting
    writes entirely — which is the exact failure this project claims to
    survive in its chaos tests.
  EOT
  type        = number
  default     = 3

  validation {
    condition     = var.availability_zone_count >= 3
    error_message = "at least three AZs are required for the Kafka availability guarantee."
  }
}

variable "kafka_broker_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_broker_storage_gb" {
  description = "EBS storage per broker."
  type        = number
  default     = 100
}

variable "eks_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.31"
}

variable "eks_node_instance_types" {
  description = <<-EOT
    Instance types for the general node group.

    Several types rather than one, so the ASG can fall back when a type is
    unavailable in an AZ. A single-type group is the usual reason a cluster
    cannot scale up during a regional capacity crunch.
  EOT
  type        = list(string)
  default     = ["m6i.xlarge", "m5.xlarge", "m6a.xlarge"]
}

variable "eks_node_min_size" {
  type    = number
  default = 2
}

variable "eks_node_max_size" {
  type    = number
  default = 8
}

variable "eks_node_desired_size" {
  type    = number
  default = 3
}

variable "lakehouse_bucket_name" {
  description = "S3 bucket for the Iceberg warehouse. Must be globally unique."
  type        = string
}
