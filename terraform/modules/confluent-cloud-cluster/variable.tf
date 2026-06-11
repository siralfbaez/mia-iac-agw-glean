variable "confluent_api_key" {
  type        = string
  description = "Confluent Cloud Cloud API Key"
  sensitive   = true
}

variable "confluent_api_secret" {
  type        = string
  description = "Confluent Cloud Cloud API Secret"
  sensitive   = true
}

variable "gcp_region" {
  type        = string
  default     = "us-central1"
  description = "GCP Region for underlying Kafka infrastructure"
}