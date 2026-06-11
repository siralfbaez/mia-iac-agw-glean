terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.0"
    }
  }
}

provider "confluent" {
  api_key    = var.confluent_api_key
  api_secret = var.confluent_api_secret
}

# 1. Create the Isolated Enterprise Environment
resource "confluent_environment" "mia_env" {
  display_name = "mia-iac-agw-glean-env"
}

# 2. Provision the Kafka Cluster on GCP with TLS 1.3/Secure endpoints
resource "confluent_kafka_cluster" "gcp_kafka" {
  display_name = "mia-secure-search-backbone"
  availability = "SINGLE_ZONE"
  cloud        = "GCP"
  region       = var.gcp_region
  type         = "STANDARD"

  environment {
    id = confluent_environment.mia_env.id
  }
}

# 3. Provision the Data Pipeline Topic
resource "confluent_kafka_topic" "unified_mutations" {
  kafka_cluster {
    id = confluent_kafka_cluster.gcp_kafka.id
  }
  topic_name       = "unified-enterprise-mutations"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.gcp_kafka.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager_keys.id
    secret = confluent_api_key.app_manager_keys.secret
  }
}

# 4. Service Account for Ingestion & Processing Agents
resource "confluent_service_account" "pipeline_agent" {
  display_name = "mia-search-pipeline-sa"
  description  = "Service Account for Go Ingest Agent and Flink Stream Processor"
}

# 5. Bind ACLs to the Service Account (Least Privilege)
resource "confluent_role_binding" "sa_kafka_cluster_admin" {
  principal   = "User:${confluent_service_account.pipeline_agent.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.gcp_kafka.rbac_crn
}

resource "confluent_api_key" "app_manager_keys" {
  display_name = "mia-pipeline-credentials"
  description  = "Kafka API Key for Go/Flink runtime engines"
  owner {
    id          = confluent_service_account.pipeline_agent.id
    api_version = confluent_service_account.pipeline_agent.api_version
    kind        = confluent_service_account.pipeline_agent.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.gcp_kafka.id
    api_version = confluent_kafka_cluster.gcp_kafka.api_version
    kind        = confluent_kafka_cluster.gcp_kafka.kind

    environment {
      id = confluent_environment.mia_env.id
    }
  }
}