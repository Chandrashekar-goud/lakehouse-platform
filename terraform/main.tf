# Optional: S3 landing bucket + least-privilege IAM user for the pipeline.
# Free-tier friendly. Alternative: create the same by hand in the console
# (docs/setup.md walks through it).
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the raw landing zone"
  type        = string
}

provider "aws" {}

resource "aws_s3_bucket" "raw" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter { prefix = "raw/" }
    expiration { days = 60 } # keeps free-tier storage bounded
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_iam_user" "pipeline" {
  name = "lakehouse-pipeline"
}

resource "aws_iam_user_policy" "pipeline_s3" {
  name = "lakehouse-raw-rw"
  user = aws_iam_user.pipeline.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

output "bucket" { value = aws_s3_bucket.raw.bucket }
output "access_key_id" {
  value     = aws_iam_access_key.pipeline.id
  sensitive = true
}
output "secret_access_key" {
  value     = aws_iam_access_key.pipeline.secret
  sensitive = true
}
