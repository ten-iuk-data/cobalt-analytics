variable "project" {
  description = "Duplicate Application Identification"
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


variable "glue_database" {
  description = "The Glue database to use"
  type        = string
}

variable "semantic_bucket" {
  description = "The S3 bucket for semantic data"
  type        = string
}

variable "semantic_input_key" {
  description = "The input key for the semantic data"
  type        = string
}

variable "cobalt_bucket" {
  description = "The S3 bucket for cobalt data"
  type        = string
}

variable "cobalt_output_duplicates" {
  description = "The output key for duplicates"
  type        = string
}

variable "cobalt_output_processed" {
  description = "The output key for processed applications"
  type        = string
}

variable "business_rules_key" {
  description = "The S3 key for business rules"
  type        = string
}

variable "sql_key" {
  description = "The S3 key for the SQL script"
  type        = string
}
variable "glue_schedule_expression" {
  description = "Cron expression for scheduling the Glue job"
  default     = "cron(30 7 * * ? *)"
}