# =============================================================================
# Heber Production Environment per PRD §22.3
# =============================================================================
# Resources per PRD §22.3:
# - EKS: 6+ nodes
# - RDS: db.r6g.large
# - S3: Cross-region replication
# - Redis: r6g.large cluster
# =============================================================================

terraform {
  # Intentionally partial: pass backend settings at init time, e.g.
  # terraform init -backend-config=backend.hcl
  backend "s3" {}
}

variable "aws_region" {
  description = "AWS region for production infrastructure"
  type        = string
}

module "heber" {
  source = "../../"

  environment        = "prod"
  region             = var.aws_region
  eks_node_count     = 6
  rds_instance_class = "db.r6g.large"
  redis_node_type    = "cache.r6g.large"
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
