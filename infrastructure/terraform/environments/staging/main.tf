# =============================================================================
# Heber Staging Environment per PRD §22.3
# =============================================================================
# Resources per PRD §22.3:
# - EKS: 3 nodes
# - RDS: db.t3.medium
# - Redis: t3.small
# =============================================================================

terraform {
  # Intentionally partial: pass backend settings at init time, e.g.
  # terraform init -backend-config=backend.hcl
  backend "s3" {}
}

variable "aws_region" {
  description = "AWS region for staging infrastructure"
  type        = string
}

module "heber" {
  source = "../../"

  environment        = "staging"
  region             = var.aws_region
  eks_node_count     = 3
  rds_instance_class = "db.t3.medium"
  redis_node_type    = "cache.t3.small"
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
