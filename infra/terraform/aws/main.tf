# AWS side of the landing zone: an encrypted, versioned S3 bucket and the IAM role that
# Snowflake's storage integration assumes to read it.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "ingestion-framework", env = var.env, owner = "data-engineering" }
  }
}

resource "aws_s3_bucket" "landing" {
  bucket = "${var.name_prefix}-landing-${var.env}"
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Landed files are loaded within minutes; keep them 30 days hot, then archive, then expire.
resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    id     = "raw-retention"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = var.retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Role assumed by Snowflake (values come from DESC INTEGRATION after the integration is created).
data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_reader" {
  name               = "${var.name_prefix}-snowflake-landing-reader-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
}

data "aws_iam_policy_document" "read_landing" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.landing.arn}/raw/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }
}

resource "aws_iam_role_policy" "snowflake_read" {
  role   = aws_iam_role.snowflake_reader.id
  policy = data.aws_iam_policy_document.read_landing.json
}

# Role for the ingestion job itself (ECS task / Batch): write-only to raw/ and its state.
data "aws_iam_policy_document" "writer" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.landing.arn}/raw/*", "${aws_s3_bucket.landing.arn}/_state/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn]
  }
}

resource "aws_iam_policy" "ingest_writer" {
  name   = "${var.name_prefix}-ingest-writer-${var.env}"
  policy = data.aws_iam_policy_document.writer.json
}
