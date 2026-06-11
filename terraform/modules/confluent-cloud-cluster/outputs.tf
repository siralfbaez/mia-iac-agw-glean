output "kafka_bootstrap_servers" {
  value       = confluent_kafka_cluster.gcp_kafka.bootstrap_endpoint
  description = "Kafka Bootstrap Connection String"
}

output "pipeline_api_key" {
  value       = confluent_api_key.app_manager_keys.id
  description = "Generated Kafka API Key for Runtime Ingestion"
}

output "pipeline_api_secret" {
  value       = confluent_api_key.app_manager_keys.secret
  sensitive   = true
  description = "Generated Kafka API Secret for Runtime Ingestion"
}