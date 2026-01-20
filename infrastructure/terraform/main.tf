# =============================================================================
# Heber Terraform Root Module per PRD §22
# =============================================================================
# Main entry point for infrastructure provisioning
# State backend: S3 + DynamoDB
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.12"
    }
  }

  # Backend configured per environment in environments/*/backend.tf
}

# =============================================================================
# Variables (set per environment)
# =============================================================================
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "eks_node_count" {
  description = "Number of EKS worker nodes"
  type        = number
}

variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
}

variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
}

# =============================================================================
# Provider Configuration
# =============================================================================
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "heber"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# =============================================================================
# Module References
# =============================================================================

# VPC for network isolation
module "vpc" {
  source = "./modules/vpc"

  environment = var.environment
  region      = var.region
}

# S3 for Parquet storage
module "s3" {
  source = "./modules/s3"

  environment = var.environment
  bucket_name = "heber-data-${var.environment}"
}

# RDS for Catalog database
module "rds" {
  source = "./modules/rds"

  environment        = var.environment
  instance_class     = var.rds_instance_class
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
}

# ElastiCache for Redis
module "elasticache" {
  source = "./modules/elasticache"

  environment        = var.environment
  node_type          = var.redis_node_type
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
}

# ECR for container images
module "ecr" {
  source = "./modules/ecr"

  environment = var.environment
  repositories = [
    "heber-consumer",
    "heber-writer",
    "heber-compactor",
    "heber-catalog",
    "heber-hotloader",
    "heber-backfill",
  ]
}

# EKS cluster
module "eks" {
  source = "./modules/eks"

  environment        = var.environment
  node_count         = var.eks_node_count
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
}

# =============================================================================
# Outputs
# =============================================================================
output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "s3_bucket_name" {
  description = "S3 bucket for Parquet storage"
  value       = module.s3.bucket_name
}

output "rds_endpoint" {
  description = "RDS endpoint for Catalog database"
  value       = module.rds.endpoint
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = module.elasticache.endpoint
}
