terraform {
  required_version = "~> 1.7.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30.0"
    }
    }
  }

  provider "aws" {
    region = "eu-west-2"
    profile = var.aws_profile
}

terraform {
  backend "s3" {
    bucket = "cobalt-analytics"
    key    = "terraform/terraform.tfstate"
    region = "eu-west-2"
    profile = "ukri_staging_keys"
  }
}