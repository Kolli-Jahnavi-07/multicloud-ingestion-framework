variable "subscription_id" {
  type = string
}

variable "location" {
  type    = string
  default = "eastus2"
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

variable "snowflake_service_principal_id" {
  description = "Object ID of Snowflake's service principal in this tenant (empty until consent is granted)"
  type        = string
  default     = ""
}
