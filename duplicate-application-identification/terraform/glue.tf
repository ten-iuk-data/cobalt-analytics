data "aws_iam_role" "existing_glue_role" {
  name = var.iam_role
}

locals {
  glue_src_path = "C:/Users/TOtt01/OneDrive - UKRI/Documents/cobalt-analytics/duplicate-application-identification/glue/"
}


resource "aws_s3_object" "duplicate_application_identification" {
  bucket = var.s3_bucket
  key    = "glue/scripts/duplicate_application_identification.py"
  source = "${local.glue_src_path}duplicate_application_identification.py"
  etag   = filemd5("${local.glue_src_path}duplicate_application_identification.py")
}

resource "aws_glue_job" "duplicate_application_identification" {
  glue_version     = "4.0"
  name             = "duplicate_application_identification"
  description      = "Identifies applications from competitions that are duplicates"
  role_arn         = data.aws_iam_role.existing_glue_role.arn
  number_of_workers = 90
  worker_type      = "G.2X"
  timeout          = 5000
  tags = {
    project = var.project
  }

  command {
    script_location = "s3://${var.s3_bucket}/glue/scripts/duplicate_application_identification.py"
  }

  default_arguments = {
    "--job-name"                = "duplicate_application_identification"
    "--class"                   = "GlueApp"
    "--enable-job-insights"     = "true"
    "--enable-auto-scaling"     = "true"
    "--enable-glue-datacatalog" = "true"
    "--enable-metrics"          = "true"
    "--job-language"            = "python"
    "--job-bookmark-option"     = "job-bookmark-disable"
    "--additional-python-modules" = "sentence-transformers"
    "--TempDir"                   = "s3://${var.s3_bucket}/glue/temp/"
    "--glue_database"             = var.glue_database
    "--semantic_bucket"           = var.semantic_bucket
    "--semantic_input_key"        = var.semantic_input_key
    "--cobalt_bucket"             = var.cobalt_bucket
    "--cobalt_output_duplicates"  = var.cobalt_output_duplicates
    "--cobalt_output_processed"   = var.cobalt_output_processed
    "--business_rules_key"        = var.business_rules_key
    "--sql_key"                   = var.sql_key
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/logs-v2"
    "--continuous-log-logStreamPrefix"   = "duplicate_application_identification/"
  }

}


resource "aws_glue_trigger" "duplicate_application_identification_trigger" {
  name     = "${aws_glue_job.duplicate_application_identification.name}-daily-trigger"
  type     = "SCHEDULED"
  schedule = var.glue_schedule_expression

  actions {
    job_name = aws_glue_job.duplicate_application_identification.name
  }

  start_on_creation = true
}


