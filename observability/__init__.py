"""
AetherFlow Observability Module
OpenTelemetry integration for distributed tracing, metrics, and structured logging

This module demonstrates enterprise-grade observability practices:
- Distributed tracing across services (Cloud Function → API → BigQuery)
- Custom metrics for business KPIs
- Structured JSON logging with correlation IDs
- Integration with Google Cloud Operations (formerly Stackdriver)
"""

from .logging import StructuredLogger, get_logger, init_logging
from .metrics import MetricType, init_metrics, record_metric
from .tracing import get_tracer, init_tracer, trace_function

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
