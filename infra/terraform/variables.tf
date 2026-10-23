variable "region" {
  description = "AWS region. us-east-1 is closest to the MTA feeds."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique name for the lake bucket (Iceberg tables, raw archive, checkpoints)."
  type        = string
}

variable "instance_type" {
  description = "One ARM instance runs the whole stack. See docs/cost.md for how this was sized."
  type        = string
  default     = "t4g.xlarge"
}

variable "monthly_budget_usd" {
  description = "Email alert when actual spend passes 80% of this."
  type        = number
  default     = 40
}

variable "alert_email" {
  description = "Where budget alerts go."
  type        = string
}

variable "raw_archive_glacier_days" {
  description = "Move raw feed segments to Glacier Instant Retrieval after this many days."
  type        = number
  default     = 30
}

variable "raw_archive_expire_days" {
  description = "Delete raw feed segments after this many days."
  type        = number
  default     = 365
}
