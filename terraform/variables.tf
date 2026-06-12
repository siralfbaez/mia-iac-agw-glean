variable "gcp_project_id" {
  type        = string
  default     = "mia-agw-glean-poc"
  description = "Target Google Cloud Project ID"
}

variable "gcp_region" {
  type        = string
  default     = "us-central1"
  description = "Target GCP Region for AI and pipeline components"
}

variable "confluent_api_key" {
  type        = string
  sensitive   = true
  description = "Confluent Cloud Infrastructure Admin Key"
}

variable "confluent_api_secret" {
  type        = string
  sensitive   = true
  description = "Confluent Cloud Infrastructure Admin Secret"
}