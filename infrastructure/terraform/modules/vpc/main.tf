terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

locals {
  # Placeholder IDs to keep root module wiring valid until full VPC resources are implemented.
  vpc_id             = "vpc-${var.environment}"
  private_subnet_ids = ["subnet-${var.environment}-a", "subnet-${var.environment}-b", "subnet-${var.environment}-c"]
}

output "vpc_id" {
  description = "VPC identifier"
  value       = local.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet identifiers"
  value       = local.private_subnet_ids
}
