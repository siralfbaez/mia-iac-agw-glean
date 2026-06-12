variable "network_name" {
  type        = string
  default     = "mia-shared-vpc-host"
  description = "The primary identity name for the foundational VPC network"
}

variable "gcp_region" {
  type        = string
  default     = "us-central1"
  description = "Target GCP region for subnet routing topologies"
}

variable "subnet_cidr" {
  type        = string
  default     = "10.0.10.0/24"
  description = "Private subnet boundary allocation for internal data routing workers"
}