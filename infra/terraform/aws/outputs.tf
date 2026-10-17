output "landing_uri" {
  value = "s3://${aws_s3_bucket.landing.bucket}/raw"
}

output "snowflake_role_arn" {
  value = aws_iam_role.snowflake_reader.arn
}

output "ingest_writer_policy_arn" {
  value = aws_iam_policy.ingest_writer.arn
}
