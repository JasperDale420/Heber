terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "node_count" {
  description = "Worker node count"
  type        = number
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

output "cluster_endpoint" {
  description = "EKS API endpoint"
  value       = "https://heber-${var.environment}.eks.local"
}
