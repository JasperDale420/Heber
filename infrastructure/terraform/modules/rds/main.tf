terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "instance_class" {
  description = "RDS instance class"
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
  description = "Catalog DB endpoint"
  value       = "heber-${var.environment}.catalog.internal:5432"
}
