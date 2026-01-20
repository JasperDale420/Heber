# =============================================================================
# Heber Staging Environment per PRD §22.3
# =============================================================================
# Resources per PRD §22.3:
# - EKS: 3 nodes
# - RDS: db.t3.medium
# - Redis: t3.small
# =============================================================================

terraform {
  backend "s3" {
    bucket         = "heber-terraform-state"
    key            = "staging/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "heber-terraform-locks"
    encrypt        = true
  }
}

module "heber" {
  source = "../../"

  environment        = "staging"
  region             = "us-east-1"
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
