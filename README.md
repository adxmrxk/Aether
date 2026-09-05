# Aether

**Serverless data lakehouse on GCP with AI-powered cryptocurrency market intelligence.**

Aether ingests cryptocurrency market data from public APIs, enriches each event with an AI sentiment score, stores vector embeddings for semantic search, lands the data in a BigQuery medallion lakehouse, and serves the results through REST, GraphQL, and WebSocket APIs running on Cloud Run.

The whole pipeline runs locally on Docker Compose against emulators, carrying live market data end to end with no cloud account. See [What Runs Where](#what-runs-where) for the three capabilities that need credentials.

---

## Table of Contents

- [Project Overview](#project-overview)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Configuration](#configuration)

---

## Project Overview

### What It Is

A production-grade, fully serverless data engineering platform that turns raw cryptocurrency market data into AI-enriched intelligence. Aether is event-driven from end to end: messages flow through Pub/Sub, processing runs on Cloud Functions Gen2, transformations execute in dbt on a scheduled cadence, and serving happens on Cloud Run. The same handlers run locally under Docker Compose against the BigQuery and Pub/Sub emulators. There are no servers to manage, infrastructure scales to zero when idle, and the entire stack is provisioned as code through Pulumi or Terraform.

### The Problem It Solves

Cryptocurrency markets generate a firehose of data: prices, volumes, on-chain events, news headlines, social signals. The raw feed is overwhelming and the analysis layer is what's valuable. Traders, researchers, and dashboards need:

- **Real-time enrichment, not batch backfills.** A sentiment score that arrives 12 hours late is useless when the market has already moved.
- **Semantic search over news, not keyword search.** A query for "ethereum scaling solutions" should return articles about L2s, rollups, and sharding even if those exact words don't match.
- **An auditable history of every transformation.** Bad data is inevitable. You need a paper trail from raw ingestion all the way to served metric so you can debug what went wrong.
- **Costs that scale with usage.** Most market intelligence platforms run expensive 24/7 clusters even when nobody is using them. A side project or research workload shouldn't require a five-figure monthly cloud bill.

Aether solves all four with the right tool at each layer: Pub/Sub for real-time event delivery, Vertex AI Gemini for inline enrichment, Pinecone for vector semantic search, a Bronze/Silver/Gold medallion lakehouse for full lineage, and a serverless compute model that genuinely scales to zero.

### What It Does

- Ingests cryptocurrency market data through Pub/Sub topics, or Kafka as an alternative transport
- Routes failed messages to a dead-letter topic after five delivery attempts (Pub/Sub's minimum)
- Enriches each event with a sentiment score and natural-language reasoning
- Generates vector embeddings and indexes them in Pinecone for semantic search
- Stores raw events in a BigQuery Bronze layer for auditability
- Transforms data through dbt models into typed Silver and aggregated Gold layers
- Serves analytics through REST endpoints on FastAPI / Cloud Run
- Exposes the same data through a GraphQL endpoint, with live subscriptions
- Streams live updates through a WebSocket server for real-time dashboards
- Caches hot reads in Redis to keep p95 latency under tight SLOs
- Exposes Prometheus metrics and ships a provisioned Grafana dashboard
- Validates data quality with dbt tests and Great Expectations-style assertions

### What Runs Where

Not every capability above is exercised by the local stack. The pipeline runs
end to end on Docker Compose with no cloud account, but three things need
credentials, and the table says so plainly rather than leaving you to find out.

| Capability | Local (Docker Compose) | Cloud (GCP) |
|---|---|---|
| Ingest, Pub/Sub, dead-letter | Yes, against the emulator | Yes |
| Kafka transport | Yes, `INGEST_TRANSPORT=kafka` | Yes |
| Cloud Function enrichment | Yes, via functions-framework | Yes |
| BigQuery Bronze + Gold | Yes, against the emulator | Yes |
| REST, GraphQL, WebSocket | Yes | Yes |
| Redis caching and fan-out | Yes | Yes |
| Prometheus + Grafana | Yes | Yes |
| **Sentiment from Gemini 1.5 Flash** | **No.** Falls back to a documented arithmetic heuristic | Yes, with a billed project |
| **Semantic search (Pinecone)** | **No.** `/api/v1/search` returns 503 | Yes, with an API key |
| **dbt Bronze -> Silver -> Gold** | **No.** dbt-bigquery needs real BigQuery; the worker aggregates Bronze into Gold instead | Yes |

The scorer that actually ran is recorded on every Bronze row in
`processing_metadata.model` (`gemini-1.5-flash-002` or `heuristic-v1`) and named
in the reasoning text, so a heuristic score is never mistaken for a model one.

---

## How It Works

The platform is split into four logical stages: ingest, enrich, transform, serve. Each stage is fully decoupled and independently scalable.

```
        ┌─────────────────────────────┐
        │  External Data Sources      │   CoinGecko, news feeds, etc.
        └──────────────┬──────────────┘
                       ▼
   1.  Pub/Sub Ingest
       Topic: aether-market-data
       Subscription: pull or push to Cloud Function
       Failure: routed to aether-dead-letter after 5 attempts
                       │
                       ▼
   2.  Cloud Function Gen2 (Python 3.12)
       functions/market_processor/main.py
       • Validates message schema
       • Scores sentiment (Vertex AI Gemini, else local heuristic)
       • Generates embedding, upserts to Pinecone
       • Writes raw payload to BigQuery Bronze table
                       │
                       ▼
   3.  BigQuery Lakehouse (Medallion Architecture)
       Bronze: raw JSON payloads, full audit trail
              │
              ▼
       dbt run (hourly, GitHub Actions scheduled)
       locally: worker aggregates Bronze -> Gold
              │
              ▼
       Silver: typed, cleaned, deduplicated records
              │
              ▼
       Gold: aggregated metrics (hourly sentiment, latest snapshot)
                       │
                       ▼
   4.  Serving Layer (Cloud Run)
       api/main.py - FastAPI
       • REST: GET /api/v1/sentiment, /api/v1/sentiment/{symbol}, ...
       • GraphQL: POST /graphql for flexible queries
       • WebSocket: live sentiment updates
       • Redis caches hot reads
       • Secret Manager supplies Pinecone and API keys
                       │
                       ▼
        ┌─────────────────────────────┐
        │  Consumers                   │
        │  Dashboards, mobile, alerts  │
        └─────────────────────────────┘
```

---

## Architecture

```
                       ┌────────────────────────────────────────┐
                       │            SOURCE LAYER                │
                       │    Public APIs (CoinGecko, news feeds) │
                       └─────────────────┬──────────────────────┘
                                         ▼
                       ┌────────────────────────────────────────┐
                       │           INGESTION LAYER              │
                       │   GCP Pub/Sub (aether-market-data)     │
                       │   Dead Letter Topic after 5 attempts   │
                       │   Kafka (local dev alternative)        │
                       └─────────────────┬──────────────────────┘
                                         ▼
        ┌──────────────────────────────────────────────────────────┐
        │                    PROCESSING LAYER                       │
        │  ┌──────────────────────┐   ┌────────────────────────┐   │
        │  │  Cloud Function Gen2 │   │  Vertex AI Gemini       │   │
        │  │  Python 3.12         │   │  Sentiment + reasoning  │   │
        │  │  Async processing    │   │                         │   │
        │  └──────────────────────┘   └────────────────────────┘   │
        │                                                            │
        │  ┌──────────────────────────────────────────────────────┐ │
        │  │  Pinecone Vector Database                             │ │
        │  │  Embeddings for semantic search                       │ │
        │  └──────────────────────────────────────────────────────┘ │
        └──────────────────────────────────────────────────────────┘
                                         ▼
        ┌──────────────────────────────────────────────────────────┐
        │                    STORAGE LAYER                          │
        │           BigQuery (Medallion Architecture)                │
        │                                                            │
        │  ┌──────────┐    ┌──────────┐    ┌──────────┐            │
        │  │  BRONZE  │ ─▶ │  SILVER  │ ─▶ │   GOLD   │            │
        │  │ raw JSON │    │  typed   │    │aggregated│            │
        │  └──────────┘    └──────────┘    └──────────┘            │
        └────────────────────┬─────────────────────────────────────┘
                             ▼
        ┌──────────────────────────────────────────────────────────┐
        │                  TRANSFORM LAYER                          │
        │   dbt-core + dbt-bigquery                                 │
        │   Staging + Gold models, schema tests, custom assertions  │
        │   Scheduled hourly via GitHub Actions                     │
        └──────────────────────┬───────────────────────────────────┘
                               ▼
        ┌──────────────────────────────────────────────────────────┐
        │                    SERVING LAYER                          │
        │   Cloud Run (FastAPI)                                     │
        │   REST + GraphQL + WebSocket endpoints                    │
        │   Redis cache, Secret Manager for credentials             │
        └──────────────────────────────────────────────────────────┘

        ┌──────────────────────────────────────────────────────────┐
        │               OBSERVABILITY LAYER                         │
        │   Prometheus  ──▶  Grafana   /   Jaeger (tracing)         │
        │   Structured JSON logs to Cloud Logging                   │
        └──────────────────────────────────────────────────────────┘

        ┌──────────────────────────────────────────────────────────┐
        │              INFRASTRUCTURE LAYER                         │
        │   Pulumi (Python, primary)  /  Terraform (HCL, alternate) │
        └──────────────────────────────────────────────────────────┘
```

---

## Tech Stack

### Application Runtime

| Component         | Technology              | Why It's Used |
|-------------------|-------------------------|---------------|
| Serving API       | **FastAPI + Uvicorn**   | Async-first Python web framework with automatic OpenAPI docs, Pydantic validation, and trivial Cloud Run packaging |
| Event Processor   | **Cloud Functions Gen2 (Python 3.12)** | Serverless, scales to zero, native Pub/Sub trigger integration with concurrent execution support |
| GraphQL Layer     | **Strawberry / FastAPI GraphQL** | Type-safe schema definition; flexible querying for clients that don't want to call N REST endpoints |
| Streaming         | **WebSocket server**    | Pushes real-time sentiment and price updates to connected dashboards without polling |

### Data Platform

| Component       | Purpose |
|-----------------|---------|
| **BigQuery**    | Serverless data warehouse for the Bronze, Silver, and Gold layers. PAY_PER_QUERY pricing fits bursty analytical workloads. |
| **dbt-core + dbt-bigquery** | Declarative SQL transformations with built-in testing, lineage, and documentation. Runs hourly via GitHub Actions. |
| **Pinecone**    | Managed vector database for semantic search over news and embeddings. Sub-100ms ANN queries at scale. |
| **Redis 7**     | Caches hot reads (latest sentiment per symbol, summary endpoints). |

### AI and ML

| Component               | Purpose |
|-------------------------|---------|
| **Vertex AI Gemini 1.5 Flash** | Per-event sentiment scoring with natural-language reasoning. Lower cost and latency than Gemini Pro for high-volume enrichment. |
| **Vector embeddings**   | Generated for every news article; indexed in Pinecone for semantic similarity search |

### Messaging and Streaming

| Component        | Purpose |
|------------------|---------|
| **GCP Pub/Sub**  | Primary event bus. At-least-once delivery, dead letter routing after 3 failed attempts, push to Cloud Functions |
| **Kafka (local)**| Local development alternative for Pub/Sub. Confluent images with Schema Registry and Kafka UI included in `docker-compose.yml` |

### Caching and State

| Component       | Purpose |
|-----------------|---------|
| **Redis**       | Low-latency cache for serving endpoints. Append-only file persistence in local dev. |
| **BigQuery materialized views** | Pre-computed Gold-layer aggregations to avoid repeated scan costs |

### Observability

| Component         | Purpose |
|-------------------|---------|
| **Prometheus**    | Scrapes metrics from the FastAPI service and any in-cluster components |
| **Grafana**       | Dashboards over Prometheus data; provisioned automatically in local Compose |
| **Jaeger**        | Distributed tracing across ingest, processing, and serve stages |
| **Structured JSON logging** | `observability/logging.py` formats logs for ingestion by Cloud Logging |

### Data Quality

| Component               | Purpose |
|-------------------------|---------|
| **dbt tests**           | Schema-level assertions enforced on every run: not-null, unique, accepted values, custom SQL tests |
| **Great Expectations-style validations** | `data_quality/expectations.py` and `validations.py` for richer assertions outside dbt |

### Infrastructure as Code

| Tool          | Purpose |
|---------------|---------|
| **Pulumi (Python)** | Primary IaC. Provisions Pub/Sub topics and subscriptions, BigQuery datasets and tables, IAM roles, service accounts. Native Python means full IDE support, classes, and pytest-based infra testing. |
| **Terraform** | Alternate IaC path for teams that prefer HCL. Same resource coverage. |

### Security

| Component                 | Purpose |
|---------------------------|---------|
| **GCP Secret Manager**    | Stores Pinecone API keys, external API credentials. Accessed by Cloud Run at startup via `api/gcp_secrets/secret_manager.py` |
| **Service Account IAM**   | Each component runs with a least-privilege service account scoped to only the resources it touches |
| **Pub/Sub Dead Letter Queue** | Three-retry threshold isolates poison-pill messages without blocking the main pipeline |

### Tooling and Quality

| Tool                | Purpose |
|---------------------|---------|
| **Poetry / setuptools** | Python packaging |
| **Ruff**            | Fast linting and import sorting (replaces flake8 + isort) |
| **mypy**            | Static type checking |
| **pytest + pytest-cov** | Unit and integration tests with coverage reporting |
| **GitHub Actions**  | CI on push, CD for Cloud Function and Cloud Run, hourly dbt runs |
| **Makefile**        | One-line shortcuts for every common task (`make dev`, `make test`, `make dbt-run`) |

---

## Project Structure

```
aether/
│
├── api/                              FastAPI Cloud Run service
│   ├── main.py                       REST endpoints, health checks, app wiring
│   ├── Dockerfile                    Multi-stage build for Cloud Run
│   ├── requirements.txt
│   ├── cache/
│   │   └── redis_cache.py            Redis client + cache helpers
│   ├── graphql_api/
│   │   └── schema.py                 GraphQL schema, queries and subscriptions
│   ├── gcp_secrets/
│   │   └── secret_manager.py         GCP Secret Manager wrapper
│   └── static/
│       └── docs.css                  Theme for the /docs API console
│
├── functions/                        Cloud Functions Gen2
│   └── market_processor/
│       ├── main.py                   Pub/Sub-triggered enricher: sentiment → Pinecone → BigQuery
│       ├── Dockerfile                Runs the same handler locally via functions-framework
│       └── requirements.txt
│
├── ingest/                           Local-only: stands in for Cloud Scheduler,
│   ├── local_ingest.py               Eventarc and dbt. Publishes CoinGecko data,
│   ├── Dockerfile                    forwards CloudEvents to the function, and
│   └── requirements.txt              aggregates Bronze into Gold.
│
├── streaming/                        Real-time push surface
│   └── websocket_server.py           Streams live sentiment to subscribed clients
│
├── kafka/                            Alternative ingest transport to Pub/Sub
│   ├── consumer.py                   Consumer, producer, DLQ, Avro schema registry
│   ├── runner.py                     Worker: consumes and forwards to the function
│   ├── Dockerfile
│   └── requirements.txt
│
├── dbt/                              BigQuery transformations
│   ├── dbt_project.yml               Project config
│   ├── profiles.yml                  Connection profile (BigQuery)
│   ├── packages.yml                  dbt package dependencies
│   ├── models/
│   │   ├── schema.yml                Column tests + documentation
│   │   ├── staging/
│   │   │   └── stg_market_data.sql   Silver layer: typed, cleaned
│   │   └── gold/
│   │       ├── gold_hourly_sentiment.sql   Aggregate sentiment per symbol per hour
│   │       └── gold_latest_sentiment.sql   Latest snapshot per symbol
│   └── tests/
│       └── assert_sentiment_score_valid_range.sql   Custom SQL test
│
├── data_quality/                     Standalone data quality checks
│   ├── expectations.py               Great Expectations-style declarations
│   └── validations.py                Validation runners
│
├── observability/                    Cross-cutting telemetry
│   ├── logging.py                    Structured JSON logging
│   ├── metrics.py                    OpenTelemetry -> Cloud Monitoring (cloud only)
│   ├── tracing.py                    OpenTelemetry / Jaeger setup
│   ├── prometheus.yml                Scrape config
│   ├── grafana/                      Provisioned datasource + Aether dashboard
│   └── requirements.txt
│
├── infra/                            Pulumi IaC (primary path)
│   ├── __main__.py                   Pub/Sub topics, BigQuery dataset/tables, IAM
│   ├── bigquery-schema.yaml          Table definitions for the local emulator
│   ├── Pulumi.yaml
│   └── requirements.txt
│
├── terraform/                        Terraform IaC (alternate path)
│   ├── main.tf
│   └── variables.tf
│
├── tests/                            Test suite
│   ├── integration/test_api.py       End-to-end API tests
│   └── requirements.txt
│
├── .github/workflows/
│   ├── deploy.yml                    Build + deploy Cloud Function and Cloud Run
│   └── dbt-run.yml                   Hourly scheduled dbt runs against BigQuery
│
├── docker-compose.yml                Full local stack (API + Kafka + Redis + observability + emulators)
├── Makefile                          Developer shortcuts for every workflow
├── pyproject.toml                    Project metadata, Ruff, mypy, pytest config
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.12+
- Docker (for the local stack)
- [Pulumi CLI](https://www.pulumi.com/docs/install/) or Terraform 1.6+
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) with a project that has billing enabled (for cloud deploys)
- A Pinecone account and API key (optional; semantic search is disabled without it)

### Local Development

The fastest way to run everything locally:

```bash
cp .env.example .env        # optional: every value has a working default
docker compose up -d
```

Within about a minute the stack is carrying live data: the ingest worker pulls
real prices from CoinGecko's public API, publishes them to the Pub/Sub emulator,
the Cloud Function enriches each message and writes it to BigQuery Bronze, and
the worker aggregates Bronze into the Gold tables the API serves. Nothing is
seeded or faked; if CoinGecko is unreachable a small offline fixture takes over
and every row it produces is labelled `offline-fixture` in its `source` column.

| Service              | URL                              | Notes |
|----------------------|----------------------------------|-------|
| API                  | http://localhost:8080            | FastAPI service |
| API console          | http://localhost:8080/docs       | Swagger UI, themed |
| GraphQL              | http://localhost:8080/graphql    | Queries and live subscriptions |
| WebSocket            | ws://localhost:8080/ws/stream    | Live sentiment and price events |
| Metrics              | http://localhost:8080/metrics    | Prometheus scrape endpoint |
| Grafana dashboard    | http://localhost:3000/d/aether-overview | Provisioned (admin / admin) |
| Prometheus           | http://localhost:9090            | Metrics queries |
| Cloud Function       | http://localhost:8083            | functions-framework |
| Kafka UI             | http://localhost:8082            | Topic and consumer inspection |
| Schema Registry      | http://localhost:8081            | Avro schema management |
| Jaeger UI            | http://localhost:16686           | Distributed traces |
| BigQuery emulator    | http://localhost:9050            | Local BigQuery target |
| Pub/Sub emulator     | http://localhost:8085            | Local Pub/Sub target |

#### How the local pipeline maps to cloud

Three cloud services have no local equivalent, so `ingest/local_ingest.py`
stands in for them. Everything else is the same code that deploys.

| Cloud | Local stand-in |
|---|---|
| Cloud Scheduler | The worker's publish loop, every `INGEST_INTERVAL_SECONDS` |
| Eventarc | The worker wraps each Pub/Sub message in the CloudEvent envelope Cloud Functions Gen2 delivers and POSTs it to functions-framework |
| dbt (Bronze -> Gold) | The worker's `rebuild_gold`, every `GOLD_REBUILD_SECONDS` |

The Cloud Function itself is the real handler from
`functions/market_processor/main.py`, the same one `make deploy-function` ships.

#### Switching to the Kafka transport

Pub/Sub and Kafka are wired identically and both end at the same function.
Only one runs at a time, because both writing would duplicate every Bronze row:

```bash
INGEST_TRANSPORT=kafka docker compose up -d ingest kafka-worker
```

Watch it with `docker compose logs -f kafka-worker`. Unset the variable to go
back to Pub/Sub.

#### Local caveats

- `/api/v1/search` returns 503. It needs Vertex AI embeddings and a Pinecone key.
- Sentiment scores come from the heuristic in the function, not Gemini.
- Grafana's sentiment panel fills in as records flow; it needs a minute of data.

For an even faster inner loop, run the API with hot reload:

```bash
make dev
```

#### Without `make`

`make` is not installed by default on Windows. Every target has a direct equivalent:

| Instead of         | Run                                                                        |
|--------------------|----------------------------------------------------------------------------|
| `make dev`         | `docker compose up -d` then `uvicorn api.main:app --reload --host 0.0.0.0 --port 8080` |
| `make install`     | `pip install -r api/requirements.txt -r ingest/requirements.txt -r functions/market_processor/requirements.txt -r tests/requirements.txt -r observability/requirements.txt` |
| `make test`        | `pytest tests/ -v`                                                          |
| `make test-cov`    | `pytest tests/ -v --cov=api --cov=functions --cov-report=html`               |
| `make lint`        | `ruff check .` and `mypy api/ --ignore-missing-imports`                      |
| `make format`      | `ruff format .` and `ruff check --fix .`                                     |
| `make docker-up`   | `docker compose up -d`                                                      |
| `make docker-down` | `docker compose down`                                                       |
| `make dbt-run`     | `cd dbt && dbt run`                                                         |
| `make dbt-test`    | `cd dbt && dbt test`                                                        |

Running the API outside Docker means the emulator hostnames don't resolve. Export `BIGQUERY_EMULATOR_HOST=localhost:9050` (and `PUBSUB_EMULATOR_HOST=localhost:8085`) so it reaches the containers on published ports.

### Cloud Deployment

#### 1. Authenticate

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

#### 2. Provision Infrastructure (Pulumi)

```bash
cd infra
python -m venv venv
source venv/bin/activate            # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt

pulumi config set gcp:project YOUR_PROJECT_ID
pulumi up
```

Or with Terraform:

```bash
make tf-init
make tf-plan
make tf-apply
```

#### 3. Deploy the Cloud Function

```bash
make deploy-function
```

#### 4. Deploy the API to Cloud Run

```bash
make deploy-api
```

#### 5. Run dbt Models

```bash
cd dbt
dbt deps
dbt run
dbt test
```

### Testing

```bash
make test           # full test suite (22 tests)
make test-cov       # with coverage
make lint           # ruff + mypy
make format         # ruff format and autofix
make dbt-test       # dbt schema and custom tests, needs a real BigQuery project
```

The suite covers the REST endpoints, GraphQL, WebSocket handshake and
subscribe/ping actions, and the cache helpers, with BigQuery mocked. It needs no
running stack. Set `REDIS_ENABLED=false` so cached responses cannot leak between
tests:

```bash
pip install -r tests/requirements.txt
REDIS_ENABLED=false pytest tests/ -v
```

### Tear Down

```bash
docker compose down -v      # local stack and volumes
make pulumi-destroy         # remove cloud resources
```

---

## API Reference

| Method | Path                                  | Purpose |
|--------|---------------------------------------|---------|
| GET    | `/health`                             | Liveness probe |
| GET    | `/api/v1/sentiment`                   | Sentiment summary across all tracked symbols |
| GET    | `/api/v1/sentiment/{symbol}`          | Current sentiment, price, and AI reasoning for one symbol |
| GET    | `/api/v1/sentiment/{symbol}/history`  | Hourly historical sentiment for a symbol |
| POST   | `/api/v1/search`                      | Semantic search across news, backed by Pinecone |
| POST   | `/graphql`                            | GraphQL endpoint for flexible queries |
| WS     | `/ws/sentiment`                       | WebSocket stream of live sentiment updates |

Example response from `/api/v1/sentiment/BTC`:

```json
{
  "symbol": "BTC",
  "sentiment_score": 7.8,
  "sentiment_category": "BULLISH",
  "sentiment_trend": "IMPROVING",
  "price_usd": 67500.00,
  "percent_change_24h": 2.5,
  "ai_reasoning": "Strong institutional accumulation with record ETF inflows over the past 48 hours.",
  "last_updated": "2026-05-02T10:00:00Z"
}
```

---

## Configuration

All configuration is environment-variable driven. The local `docker-compose.yml` injects defaults; production deploys pull from GCP Secret Manager via `api/gcp_secrets/secret_manager.py`.

### Required

| Variable           | Description                              |
|--------------------|------------------------------------------|
| `GCP_PROJECT_ID`   | GCP project for Pub/Sub, BigQuery, Vertex AI |
| `PINECONE_API_KEY` | API key for Pinecone vector index        |
| `PINECONE_INDEX`   | Pinecone index name (default: `aether-market-vectors`) |

### Optional

| Variable                  | Description                                 | Default                |
|---------------------------|---------------------------------------------|------------------------|
| `GCP_LOCATION`            | GCP region                                  | `us-central1`          |
| `BIGQUERY_DATASET`        | BigQuery dataset name                       | `aether_lakehouse`     |
| `REDIS_HOST`              | Redis hostname                              | `redis`                |
| `REDIS_PORT`              | Redis port                                  | `6379`                 |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers                               | `kafka:29092`          |
| `ENVIRONMENT`             | `development`, `staging`, or `production`   | `development`          |
| `BIGQUERY_EMULATOR_HOST`  | Point BigQuery at a local emulator          | unset (real BigQuery)  |
| `INGEST_TRANSPORT`        | `pubsub` or `kafka`                         | `pubsub`               |
| `INGEST_COINS`            | CoinGecko ids to track, comma separated     | 6 majors               |
| `INGEST_INTERVAL_SECONDS` | Seconds between upstream fetches            | `60`                   |
| `GOLD_REBUILD_SECONDS`    | Seconds between Bronze to Gold rebuilds     | `30`                   |
| `PUBSUB_MAX_ATTEMPTS`     | Deliveries before dead-lettering (min 5)    | `5`                    |
| `REDIS_ENABLED`           | Set `false` to disable caching (tests)      | `true`                 |
| `STREAM_CHANNEL`          | Redis channel for live fan-out              | `aether:stream`        |
