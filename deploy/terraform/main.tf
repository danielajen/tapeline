# Tapeline infrastructure.
#
#   terraform init -backend-config=env/dev.backend.hcl
#   terraform plan  -var-file=env/dev.tfvars
#   terraform apply -var-file=env/dev.tfvars
#
# Everything here is small on purpose. This is a portfolio deployment that
# gets torn down between demos, so the defaults are the smallest shapes that
# still preserve the properties being demonstrated — three AZs for the Kafka
# availability guarantee, three brokers for replication factor 3 — rather
# than the smallest shapes that merely run.
#
# Run `terraform destroy` when you are done. MSK and NAT gateways bill by the
# hour whether or not anything is using them, and they are the two line items
# that turn a demo into a surprise.

locals {
  name = "tapeline-${var.environment}"

  # Public subnets host only the NAT gateways and the load balancers.
  # Everything that holds or processes data sits in private subnets with no
  # inbound route from the internet.
  azs = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)

  tags = {
    Project     = "tapeline"
    Environment = var.environment
  }
}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

data "aws_caller_identity" "current" {}
