bucket         = "heber-terraform-state"
key            = "staging/terraform.tfstate"
dynamodb_table = "heber-terraform-locks"
encrypt        = true

# Region comes from AWS SDK settings (AWS_REGION / AWS_DEFAULT_REGION)
