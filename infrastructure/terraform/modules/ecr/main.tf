terraform {
  required_version = ">= 1.5.0"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "repositories" {
  description = "Repository names"
  type        = list(string)
}

output "repository_urls" {
  description = "ECR repository URL map"
  value = {
    for repo in var.repositories :
    repo => "000000000000.dkr.ecr.us-east-1.amazonaws.com/${repo}-${var.environment}"
  }
}
