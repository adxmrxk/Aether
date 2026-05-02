"""
AetherFlow Infrastructure - Pulumi (Python)
Serverless Data Lakehouse on GCP with Dead Letter Queue support

Why Pulumi over Terraform?
- Native Python: Leverage Python's testing frameworks (pytest) for infrastructure validation
- Type Safety: Full IDE autocomplete and type checking
- Reusable Components: Create classes/functions for infrastructure patterns
- State Management: Built-in state management with Pulumi Cloud
"""

import pulumi
from pulumi_gcp import (
    bigquery,
    pubsub,
    serviceaccount,
    projects,
    cloudfunctionsv2,
    storage,
)

# Configuration
config = pulumi.Config()
gcp_config = pulumi.Config("gcp")
project = gcp_config.require("project")
region = gcp_config.get("region") or "us-central1"

# ============================================================================
# Service Account (Least Privilege)
# ============================================================================

aether_sa = serviceaccount.Account(
    "aether-processor-sa",
    account_id="aether-processor",
    display_name="Aether Data Processor Service Account",
    description="Service account for AetherFlow data processing pipeline",
)

# IAM Roles for the service account
sa_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.subscriber",
    "roles/pubsub.publisher",  # For DLQ publishing
    "roles/aiplatform.user",   # Vertex AI access
]

for i, role in enumerate(sa_roles):
    projects.IAMMember(
        f"aether-sa-role-{i}",
        project=project,
        role=role,
        member=aether_sa.email.apply(lambda email: f"serviceAccount:{email}"),
    )

# ============================================================================
# BigQuery Dataset & Tables (Bronze/Silver/Gold Architecture)
# ============================================================================

dataset = bigquery.Dataset(
    "aether-dataset",
    dataset_id="aether_lakehouse",
    friendly_name="Aether Data Lakehouse",
    description="Serverless Data Lakehouse for crypto market data and AI insights",
    location="US",
    default_table_expiration_ms=None,  # No expiration for lakehouse
    labels={
        "environment": "production",
        "managed_by": "pulumi",
    },
)

# Bronze Table - Raw ingested data
bronze_table = bigquery.Table(
    "bronze-raw-data",
    dataset_id=dataset.dataset_id,
    table_id="bronze_raw_data",
    deletion_protection=False,
    schema=pulumi.Output.all().apply(lambda _: """[
        {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Unique message ID"
        },
        {
            "name": "raw_payload",
            "type": "JSON",
            "mode": "REQUIRED",
            "description": "Raw JSON payload from data source"
        },
        {
            "name": "source",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "Data source identifier"
        },
        {
            "name": "ingested_at",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
            "description": "Ingestion timestamp"
        },
        {
            "name": "sentiment_score",
            "type": "FLOAT64",
            "mode": "NULLABLE",
            "description": "AI-generated sentiment score (1-10)"
        },
        {
            "name": "sentiment_reasoning",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "AI reasoning for sentiment score"
        },
        {
            "name": "processing_metadata",
            "type": "JSON",
            "mode": "NULLABLE",
            "description": "Processing metadata and errors"
        }
    ]"""),
    time_partitioning=bigquery.TableTimePartitioningArgs(
        type="DAY",
        field="ingested_at",
    ),
    clustering=["source"],
    labels={
        "layer": "bronze",
        "managed_by": "pulumi",
    },
)

# Silver Table - Cleaned and typed data (created by dbt, schema reference only)
silver_table = bigquery.Table(
    "silver-market-data",
    dataset_id=dataset.dataset_id,
    table_id="silver_market_data",
    deletion_protection=False,
    schema=pulumi.Output.all().apply(lambda _: """[
        {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "symbol",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "Cryptocurrency symbol (e.g., BTC, ETH)"
        },
        {
            "name": "price_usd",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "volume_24h",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "market_cap",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "percent_change_24h",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "sentiment_score",
            "type": "FLOAT64",
            "mode": "NULLABLE"
        },
        {
            "name": "sentiment_reasoning",
            "type": "STRING",
            "mode": "NULLABLE"
        },
        {
            "name": "ingested_at",
            "type": "TIMESTAMP",
            "mode": "REQUIRED"
        },
        {
            "name": "processed_at",
            "type": "TIMESTAMP",
            "mode": "REQUIRED"
        }
    ]"""),
    time_partitioning=bigquery.TableTimePartitioningArgs(
        type="DAY",
        field="ingested_at",
    ),
    clustering=["symbol"],
    labels={
        "layer": "silver",
        "managed_by": "pulumi",
    },
)

# ============================================================================
# Pub/Sub Topics & Subscriptions with Dead Letter Queue
# ============================================================================

# Dead Letter Topic (receives failed messages after 3 retries)
dead_letter_topic = pubsub.Topic(
    "aether-dead-letter",
    name="aether-dead-letter",
    message_retention_duration="604800s",  # 7 days retention
    labels={
        "purpose": "dead-letter-queue",
        "managed_by": "pulumi",
    },
)

# Dead Letter Subscription (for monitoring/reprocessing failed messages)
dead_letter_subscription = pubsub.Subscription(
    "aether-dead-letter-sub",
    name="aether-dead-letter-sub",
    topic=dead_letter_topic.name,
    ack_deadline_seconds=60,
    message_retention_duration="604800s",  # 7 days
    retain_acked_messages=True,  # Keep for debugging
    expiration_policy=pubsub.SubscriptionExpirationPolicyArgs(
        ttl="",  # Never expire
    ),
    labels={
        "purpose": "dlq-monitoring",
        "managed_by": "pulumi",
    },
)

# Main Ingestion Topic
market_data_topic = pubsub.Topic(
    "aether-market-data",
    name="aether-market-data",
    message_retention_duration="86400s",  # 24 hours
    labels={
        "purpose": "market-data-ingestion",
        "managed_by": "pulumi",
    },
)

# Main Subscription with DLQ configuration
# Messages are sent to DLQ after 3 failed delivery attempts
market_data_subscription = pubsub.Subscription(
    "aether-market-data-sub",
    name="aether-market-data-sub",
    topic=market_data_topic.name,
    ack_deadline_seconds=300,  # 5 minutes for AI processing
    message_retention_duration="86400s",
    retry_policy=pubsub.SubscriptionRetryPolicyArgs(
        minimum_backoff="10s",
        maximum_backoff="600s",  # 10 minutes max backoff
    ),
    dead_letter_policy=pubsub.SubscriptionDeadLetterPolicyArgs(
        dead_letter_topic=dead_letter_topic.id,
        max_delivery_attempts=3,  # Send to DLQ after 3 failures
    ),
    expiration_policy=pubsub.SubscriptionExpirationPolicyArgs(
        ttl="",  # Never expire
    ),
    labels={
        "purpose": "market-data-processing",
        "managed_by": "pulumi",
    },
)

# Grant Pub/Sub service account permission to publish to DLQ
pubsub_sa_dlq_publisher = pubsub.TopicIAMMember(
    "pubsub-sa-dlq-publisher",
    topic=dead_letter_topic.name,
    role="roles/pubsub.publisher",
    member=f"serviceAccount:service-{project}@gcp-sa-pubsub.iam.gserviceaccount.com",
)

# ============================================================================
# Cloud Storage Bucket (for Cloud Function source)
# ============================================================================

function_bucket = storage.Bucket(
    "aether-functions-bucket",
    name=f"aether-functions-{project}",
    location="US",
    uniform_bucket_level_access=True,
    labels={
        "purpose": "cloud-functions",
        "managed_by": "pulumi",
    },
)

# ============================================================================
# Exports
# ============================================================================

pulumi.export("service_account_email", aether_sa.email)
pulumi.export("dataset_id", dataset.dataset_id)
pulumi.export("bronze_table_id", bronze_table.table_id)
pulumi.export("silver_table_id", silver_table.table_id)
pulumi.export("market_data_topic", market_data_topic.name)
pulumi.export("market_data_subscription", market_data_subscription.name)
pulumi.export("dead_letter_topic", dead_letter_topic.name)
pulumi.export("dead_letter_subscription", dead_letter_subscription.name)
pulumi.export("function_bucket", function_bucket.name)
