terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "bucket_name" {
  description = "Data bucket name"
  type        = string
}

output "bucket_name" {
  description = "S3 bucket name"
  value       = var.bucket_name
}
