"""
AetherFlow Observability Module
OpenTelemetry integration for distributed tracing, metrics, and structured logging

This module demonstrates enterprise-grade observability practices:
- Distributed tracing across services (Cloud Function → API → BigQuery)
- Custom metrics for business KPIs
- Structured JSON logging with correlation IDs
- Integration with Google Cloud Operations (formerly Stackdriver)
"""

from .tracing import init_tracer, get_tracer, trace_function
from .metrics import init_metrics, record_metric, MetricType
from .logging import init_logging, get_logger, StructuredLogger

__all__ = [
    "init_tracer",
    "get_tracer",
    "trace_function",
    "init_metrics",
    "record_metric",
    "MetricType",
    "init_logging",
    "get_logger",
    "StructuredLogger",
]
