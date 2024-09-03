terraform {
  required_version = "~> 1.6.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30.0"
    }
    ## ..
    }
  }

  provider "aws" {
    region = "eu-west-2"
    profile = "default"
}

terraform {
  backend "s3" {
    bucket = "cobalt-ml"
    key    = "terraform/terraform.tfstate"
    region = "eu-west-2"
    profile = "default"
  }
}