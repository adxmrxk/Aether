# AetherFlow Infrastructure - Terraform
# Alternative IaC implementation demonstrating multi-tool proficiency
#
# This Terraform configuration mirrors the Pulumi implementation,
# showing ability to work with both infrastructure tools.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }

  # Remote state in GCS (production)
  backend "gcs" {
    bucket = "aether-terraform-state"
    prefix = "terraform/state"
  }
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Provider configuration
provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Local values
locals {
  service_name = "aether"
  labels = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "aether"
  }
}

# ============================================================================
# Service Account (Least Privilege)
# ============================================================================

resource "google_service_account" "processor" {
  account_id   = "aether-processor"
  display_name = "Aether Data Processor"
  description  = "Service account for AetherFlow data processing pipeline"
}

# IAM roles for service account
resource "google_project_iam_member" "processor_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher",
    "roles/aiplatform.user",
    "roles/secretmanager.secretAccessor",
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.processor.email}"
}

# ============================================================================
# Pub/Sub Topics & Subscriptions
# ============================================================================

# Dead Letter Topic
resource "google_pubsub_topic" "dead_letter" {
  name = "aether-dead-letter"

  message_retention_duration = "604800s" # 7 days

  labels = local.labels
}

# Dead Letter Subscription
resource "google_pubsub_subscription" "dead_letter" {
  name  = "aether-dead-letter-sub"
  topic = google_pubsub_topic.dead_letter.name

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = true

  expiration_policy {
    ttl = "" # Never expire
  }

  labels = local.labels
}

# Main Market Data Topic
resource "google_pubsub_topic" "market_data" {
  name = "aether-market-data"

  message_retention_duration = "86400s" # 24 hours

  labels = local.labels
}

# Main Subscription with DLQ
resource "google_pubsub_subscription" "market_data" {
  name  = "aether-market-data-sub"
  topic = google_pubsub_topic.market_data.name

  ack_deadline_seconds       = 300 # 5 minutes for AI processing
  message_retention_duration = "86400s"

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 3
  }

  expiration_policy {
    ttl = "" # Never expire
  }

  labels = local.labels
}

# Grant Pub/Sub SA permission to publish to DLQ
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  topic  = google_pubsub_topic.dead_letter.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# ============================================================================
# BigQuery Dataset & Tables
# ============================================================================

resource "google_bigquery_dataset" "lakehouse" {
  dataset_id  = "aether_lakehouse"
  description = "Serverless Data Lakehouse for crypto market data"
  location    = "US"

  labels = local.labels
}

# Bronze Table - Raw Data
resource "google_bigquery_table" "bronze" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "bronze_raw_data"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["source"]

  schema = jsonencode([
    {
      name        = "id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique message ID"
    },
    {
      name        = "raw_payload"
      type        = "JSON"
      mode        = "REQUIRED"
      description = "Raw JSON payload"
    },
    {
      name        = "source"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Data source identifier"
    },
    {
      name        = "ingested_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Ingestion timestamp"
    },
    {
      name        = "sentiment_score"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "AI sentiment score (1-10)"
    },
    {
      name        = "sentiment_reasoning"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "AI reasoning"
    },
    {
      name        = "processing_metadata"
      type        = "JSON"
      mode        = "NULLABLE"
      description = "Processing metadata"
    }
  ])

  labels = merge(local.labels, { layer = "bronze" })
}

# Silver Table - Cleaned Data
resource "google_bigquery_table" "silver" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "silver_market_data"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "id", type = "STRING", mode = "REQUIRED" },
    { name = "symbol", type = "STRING", mode = "REQUIRED" },
    { name = "price_usd", type = "FLOAT64", mode = "NULLABLE" },
    { name = "volume_24h", type = "FLOAT64", mode = "NULLABLE" },
    { name = "market_cap", type = "FLOAT64", mode = "NULLABLE" },
    { name = "percent_change_24h", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sentiment_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sentiment_reasoning", type = "STRING", mode = "NULLABLE" },
    { name = "ingested_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "processed_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])

  labels = merge(local.labels, { layer = "silver" })
}

# Data Quality Log Table
resource "google_bigquery_table" "data_quality_log" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "data_quality_log"
  deletion_protection = false

  schema = jsonencode([
    { name = "checkpoint_name", type = "STRING", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "success_percent", type = "FLOAT64", mode = "REQUIRED" },
    { name = "failed_count", type = "INT64", mode = "REQUIRED" },
    { name = "failed_expectations", type = "JSON", mode = "NULLABLE" },
    { name = "run_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "table_name", type = "STRING", mode = "REQUIRED" },
    { name = "row_count", type = "INT64", mode = "REQUIRED" },
    { name = "metadata", type = "JSON", mode = "NULLABLE" }
  ])

  labels = local.labels
}

# ============================================================================
# Cloud Memorystore (Redis)
# ============================================================================

resource "google_redis_instance" "cache" {
  name           = "aether-cache"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = var.region

  redis_version = "REDIS_7_0"

  auth_enabled            = true
  transit_encryption_mode = "SERVER_AUTHENTICATION"

  labels = local.labels
}

# ============================================================================
# Secret Manager
# ============================================================================

resource "google_secret_manager_secret" "pinecone_api_key" {
  secret_id = "pinecone-api-key"

  replication {
    auto {}
  }

  labels = local.labels
}

resource "google_secret_manager_secret" "redis_password" {
  secret_id = "redis-password"

  replication {
    auto {}
  }

  labels = local.labels
}

# ============================================================================
# Cloud Storage (Function Source)
# ============================================================================

resource "google_storage_bucket" "functions" {
  name     = "aether-functions-${var.project_id}"
  location = "US"

  uniform_bucket_level_access = true

  labels = local.labels
}

# ============================================================================
# Artifact Registry (Container Images)
# ============================================================================

resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "aether"
  description   = "AetherFlow container images"
  format        = "DOCKER"

  labels = local.labels
}

# ============================================================================
# Data Sources
# ============================================================================

data "google_project" "current" {
  project_id = var.project_id
}

# ============================================================================
# Outputs
# ============================================================================

output "service_account_email" {
  description = "Processor service account email"
  value       = google_service_account.processor.email
}

output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.lakehouse.dataset_id
}

output "market_data_topic" {
  description = "Market data Pub/Sub topic"
  value       = google_pubsub_topic.market_data.name
}

output "dead_letter_topic" {
  description = "Dead letter Pub/Sub topic"
  value       = google_pubsub_topic.dead_letter.name
}

output "redis_host" {
  description = "Redis instance host"
  value       = google_redis_instance.cache.host
}

output "redis_port" {
  description = "Redis instance port"
  value       = google_redis_instance.cache.port
}

output "artifact_registry" {
  description = "Artifact Registry repository"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.containers.repository_id}"
}
