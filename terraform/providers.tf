terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.0"
    }
  }

  # Configured for a secure GCS backend bucket for state locking
  backend "gcs" {
    bucket = "mia-iac-agw-glean-tfstate"
    prefix = "terraform/state/poc"
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}