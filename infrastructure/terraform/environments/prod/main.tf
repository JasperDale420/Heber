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
  backend "s3" {
    bucket         = "heber-terraform-state"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "heber-terraform-locks"
    encrypt        = true
  }
}

module "heber" {
  source = "../../"

  environment        = "prod"
  region             = "us-east-1"
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
