data "aws_iam_role" "existing_glue_role" {
  name = var.iam_role
}

resource "aws_s3_object" "duplicate_application_identification" {
  bucket = var.s3_bucket
  key    = "glue/scripts/duplicate_application_identification.py"
  source = "${local.glue_src_path}duplicate_application_identification.py"
  etag   = filemd5("${local.glue_src_path}duplicate_application_identification.py")
}

resource "aws_glue_job" "duplicate_application_identification" {
  glue_version     = "4.0"
  max_retries      = 3
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
    "--class"                   = "GlueApp"
    "--enable-job-insights"     = "true"
    "--enable-auto-scaling"     = "true"
    "--enable-glue-datacatalog" = "true"
    "--enable-metrics"          = "true"
    "--job-language"            = "python"
    "--job-bookmark-option"     = "job-bookmark-disable"
    "--additional-python-modules" = "sentence-transformers"
    "--TempDir"                   = "s3://${var.s3_bucket}/glue/temp/"
  }
}