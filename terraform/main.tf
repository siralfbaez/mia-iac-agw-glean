# 1. Instantiate the Confluent Cloud Event Mesh Backbone Module
module "confluent_backbone" {
  source               = "./modules/confluent-cloud-cluster"
  confluent_api_key    = var.confluent_api_key
  confluent_api_secret = var.confluent_api_secret
  gcp_region           = var.gcp_region
}

# 2. Provision Hardened IAM Service Account for Flink Stream Worker
resource "google_service_account" "flink_worker_sa" {
  account_id   = "mia-flink-stream-worker"
  display_name = "Hardened Flink Stream Worker SA"
  project      = var.gcp_project_id
}

# 3. Grant Vertex AI User Privileges (Principle of Least Privilege - NIST 800-53)
resource "google_project_iam_member" "vertex_ai_user" {
  project = var.gcp_project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.flink_worker_sa.email}"
}

# 4. Grant Pub/Sub Viewer if legacy source fallbacks are triggered
resource "google_project_iam_member" "pubsub_viewer" {
  project = var.gcp_project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.flink_worker_sa.email}"
}