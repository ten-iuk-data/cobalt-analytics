terraform {
  required_version = "~> 1.7.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30.0"
    }
  }

    backend "s3" {
      bucket = "placeholder"
      key    = "placeholder"
      region = "eu-west-2"
    }
}

provider "aws" {
  region  = "eu-west-2"
  profile = var.aws_profile
}