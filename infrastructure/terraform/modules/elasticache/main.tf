terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "node_type" {
  description = "Redis node type"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

output "endpoint" {
  description = "Redis endpoint"
  value       = "heber-${var.environment}.redis.internal:6379"
}
