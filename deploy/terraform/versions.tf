terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # Remote state with locking. Local state is fine for exactly one person on
  # exactly one machine, and stops being fine the moment anything else — CI,
  # a second laptop — also runs apply. The DynamoDB lock is what prevents two
  # applies from interleaving and corrupting state.
  backend "s3" {
    key            = "tapeline/terraform.tfstate"
    encrypt        = true
    dynamodb_table = "tapeline-terraform-locks"
    # bucket and region come from -backend-config so this file is not
    # environment-specific.
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "tapeline"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
