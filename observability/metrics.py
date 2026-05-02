"""
OpenTelemetry Metrics Module

Custom metrics for monitoring AetherFlow performance and business KPIs:
- Request latency histograms
- Sentiment score distributions
- Processing throughput counters
- Error rate tracking
- BigQuery query performance
- Pinecone search latency

Exports metrics to Google Cloud Monitoring for dashboards and alerting.
"""

import os
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Generator, Optional

from opentelemetry import metrics
from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

# Global meter instance
_meter: Optional[metrics.Meter] = None

# Metric instruments cache
_instruments: dict[str, Any] = {}


class MetricType(Enum):
    """Types of metrics supported."""
    COUNTER = "counter"
    UP_DOWN_COUNTER = "up_down_counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"


def init_metrics(
    service_name: str,
    service_version: str = "1.0.0",
    export_interval_ms: int = 60000,
) -> metrics.Meter:
    """
    Initialize OpenTelemetry metrics with Google Cloud Monitoring exporter.

    Args:
        service_name: Name of the service
        service_version: Semantic version
        export_interval_ms: Metric export interval in milliseconds

    Returns:
        Configured meter instance
    """
    global _meter

    # Create resource
    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version,
    })

    # Configure exporter for GCP
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        exporter = CloudMonitoringMetricsExporter()
        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=export_interval_ms,
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
    else:
        # Local development: no export
        provider = MeterProvider(resource=resource)

    metrics.set_meter_provider(provider)
    _meter = metrics.get_meter(service_name, service_version)

    # Initialize standard metrics
    _init_standard_metrics()

    return _meter


def _init_standard_metrics() -> None:
    """Initialize standard AetherFlow metrics."""
    global _instruments

    meter = get_meter()

    # Request metrics
    _instruments["request_count"] = meter.create_counter(
        name="aether.requests.total",
        description="Total number of API requests",
        unit="1",
    )

    _instruments["request_latency"] = meter.create_histogram(
        name="aether.requests.latency",
        description="Request latency in milliseconds",
        unit="ms",
    )

    # Processing metrics
    _instruments["messages_processed"] = meter.create_counter(
        name="aether.messages.processed",
        description="Total messages processed by Cloud Function",
        unit="1",
    )

    _instruments["processing_latency"] = meter.create_histogram(
        name="aether.processing.latency",
        description="Message processing latency in milliseconds",
        unit="ms",
    )

    # Sentiment metrics
    _instruments["sentiment_score"] = meter.create_histogram(
        name="aether.sentiment.score",
        description="Distribution of sentiment scores",
        unit="1",
    )

    _instruments["sentiment_requests"] = meter.create_counter(
        name="aether.sentiment.requests",
        description="Vertex AI sentiment analysis requests",
        unit="1",
    )

    # Error metrics
    _instruments["errors"] = meter.create_counter(
        name="aether.errors.total",
        description="Total errors by type",
        unit="1",
    )

    # BigQuery metrics
    _instruments["bigquery_latency"] = meter.create_histogram(
        name="aether.bigquery.latency",
        description="BigQuery query latency in milliseconds",
        unit="ms",
    )

    _instruments["bigquery_rows"] = meter.create_counter(
        name="aether.bigquery.rows",
        description="Rows inserted/queried from BigQuery",
        unit="1",
    )

    # Pinecone metrics
    _instruments["pinecone_latency"] = meter.create_histogram(
        name="aether.pinecone.latency",
        description="Pinecone operation latency in milliseconds",
        unit="ms",
    )

    _instruments["pinecone_vectors"] = meter.create_counter(
        name="aether.pinecone.vectors",
        description="Vectors upserted/queried from Pinecone",
        unit="1",
    )

    # Active connections (gauge-like using up_down_counter)
    _instruments["active_connections"] = meter.create_up_down_counter(
        name="aether.connections.active",
        description="Active connections",
        unit="1",
    )


def get_meter() -> metrics.Meter:
    """Get the initialized meter instance."""
    global _meter
    if _meter is None:
        _meter = metrics.get_meter("aether-default")
    return _meter


def record_metric(
    name: str,
    value: float,
    attributes: Optional[dict[str, str]] = None,
    metric_type: MetricType = MetricType.COUNTER,
) -> None:
    """
    Record a metric value.

    Args:
        name: Metric name (from _instruments or custom)
        value: Metric value
        attributes: Dimension attributes
        metric_type: Type of metric operation
    """
    attrs = attributes or {}

    if name in _instruments:
        instrument = _instruments[name]

        if metric_type == MetricType.COUNTER:
            instrument.add(value, attrs)
        elif metric_type == MetricType.HISTOGRAM:
            instrument.record(value, attrs)
        elif metric_type == MetricType.UP_DOWN_COUNTER:
            instrument.add(value, attrs)


@contextmanager
def measure_latency(
    metric_name: str,
    attributes: Optional[dict[str, str]] = None,
) -> Generator[None, None, None]:
    """
    Context manager to measure operation latency.

    Example:
        with measure_latency("bigquery_latency", {"operation": "insert"}):
            client.insert_rows(...)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        record_metric(metric_name, elapsed_ms, attributes, MetricType.HISTOGRAM)


def increment_counter(
    name: str,
    value: int = 1,
    attributes: Optional[dict[str, str]] = None,
) -> None:
    """Convenience function to increment a counter."""
    record_metric(name, value, attributes, MetricType.COUNTER)


def record_error(
    error_type: str,
    service: str,
    operation: str,
) -> None:
    """Record an error metric with standard attributes."""
    increment_counter("errors", 1, {
        "error.type": error_type,
        "service": service,
        "operation": operation,
    })
