variable "region" {
  type    = string
  default = "us-east-2"
}

variable "env" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "test", "prod"], var.env)
    error_message = "env must be dev, test or prod."
  }
}

variable "name_prefix" {
  type    = string
  default = "acme-loans"
}

variable "retention_days" {
  type    = number
  default = 400
}

variable "snowflake_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION AWS_LANDING_INT"
  type        = string
}

variable "snowflake_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION AWS_LANDING_INT"
  type        = string
}
