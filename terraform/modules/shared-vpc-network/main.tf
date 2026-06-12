# 1. Base VPC Network Construction
resource "google_compute_network" "shared_vpc" {
  name                    = var.network_name
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

# 2. Hardened Private Subnet
resource "google_compute_subnetwork" "private_subnet" {
  name                     = "mia-secure-pipeline-subnet"
  ip_cidr_range            = var.subnet_cidr
  region                   = var.gcp_region
  network                  = google_compute_network.shared_vpc.id

  # Crucial NIST Control: Enables internal access to Google APIs without public IPs
  private_ip_google_access = true
}

# 3. Cloud Router for Outbound Traffic Orchestration
resource "google_compute_router" "nat_router" {
  name    = "mia-nat-traffic-router"
  region  = var.gcp_region
  network = google_compute_network.shared_vpc.id
}

# 4. Cloud NAT Gateway Configuration (Secure Outbound Egress)
resource "google_compute_router_nat" "vpc_nat" {
  name                               = "mia-vpc-nat-gateway"
  router                             = google_compute_router.nat_router.name
  region                             = var.gcp_region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.private_subnet.id
    source_ip_ranges_to_nat = ["PRIMARY_IP_RANGE"]
  }

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}