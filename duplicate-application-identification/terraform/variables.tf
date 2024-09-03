variable "project" {
  description = "The project name"
  type        = string
}

variable "s3_bucket" {
  description = "The S3 bucket name for the Glue job scripts and data"
  type        = string
}

variable "iam_role" {
  description = "The IAM role to be used for the Glue job"
  type        = string
}

variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "eu-west-2"
}

variable "aws_profile" {
  description = "The AWS CLI profile to use"
  type        = string
  default     = "default"
}