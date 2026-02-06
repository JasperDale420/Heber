# =============================================================================
# Heber Dev Environment per PRD §22.3
# =============================================================================
# Resources per PRD §22.3:
# - EKS: 2 nodes
# - RDS: db.t3.small
# - Redis: t3.micro
# =============================================================================

terraform {
  # Intentionally partial: pass backend settings at init time, e.g.
  # terraform init -backend-config=backend.hcl
  backend "s3" {}
}

variable "aws_region" {
  description = "AWS region for dev infrastructure"
  type        = string
}

module "heber" {
  source = "../../"

  environment        = "dev"
  region             = var.aws_region
  eks_node_count     = 2
  rds_instance_class = "db.t3.small"
  redis_node_type    = "cache.t3.micro"
}

output "eks_cluster_endpoint" {
  value = module.heber.eks_cluster_endpoint
}

output "s3_bucket_name" {
  value = module.heber.s3_bucket_name
}

output "rds_endpoint" {
  value = module.heber.rds_endpoint
}

output "redis_endpoint" {
  value = module.heber.redis_endpoint
}
