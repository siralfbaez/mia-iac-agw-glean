output "network_id" {
  value       = google_compute_network.shared_vpc.id
  description = "The fully qualified unique identifier of the provisioned VPC"
}

output "subnet_id" {
  value       = google_compute_subnetwork.private_subnet.id
  description = "The fully qualified unique identifier of the secure routing subnet"
}