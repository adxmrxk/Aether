"""
OpenTelemetry Distributed Tracing Module

Provides distributed tracing across all AetherFlow services:
- Cloud Functions
- FastAPI endpoints
- BigQuery operations
- Pinecone queries
- External API calls

Exports traces to Google Cloud Trace for visualization and analysis.
"""

import functools
import os
from typing import Any, Callable, Optional

from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.cloud_trace_propagator import CloudTraceFormatPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode, Span

# Global tracer instance
_tracer: Optional[trace.Tracer] = None


def init_tracer(
    service_name: str,
    service_version: str = "1.0.0",
    environment: str = "production",
    use_batch_processor: bool = True,
) -> trace.Tracer:
    """
    Initialize OpenTelemetry tracer with Google Cloud Trace exporter.

    Args:
        service_name: Name of the service (e.g., "aether-api", "aether-processor")
        service_version: Semantic version of the service
        environment: Deployment environment (dev, staging, production)
        use_batch_processor: Use batch processing for better performance

    Returns:
        Configured tracer instance
    """
    global _tracer

    # Create resource with service metadata
    resource = Resource.create({
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment": environment,
        "cloud.provider": "gcp",
        "cloud.platform": "gcp_cloud_run",
    })

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # Configure exporter based on environment
    if os.environ.get("GOOGLE_CLOUD_PROJECT"):
        # Production: Export to Cloud Trace
        exporter = CloudTraceSpanExporter()

        if use_batch_processor:
            processor = BatchSpanProcessor(exporter)
        else:
            processor = SimpleSpanProcessor(exporter)

        provider.add_span_processor(processor)

    # Set global tracer provider
    trace.set_tracer_provider(provider)

    # Set Cloud Trace propagator for distributed tracing
    set_global_textmap(CloudTraceFormatPropagator())

    # Auto-instrument HTTP requests
    RequestsInstrumentor().instrument()

    # Create and cache tracer
    _tracer = trace.get_tracer(service_name, service_version)

    return _tracer


def get_tracer() -> trace.Tracer:
    """Get the initialized tracer instance."""
    global _tracer
    if _tracer is None:
        _tracer = trace.get_tracer("aether-default")
    return _tracer


def trace_function(
    span_name: Optional[str] = None,
    attributes: Optional[dict[str, Any]] = None,
    record_exception: bool = True,
):
    """
    Decorator to automatically trace a function.

    Args:
        span_name: Custom span name (defaults to function name)
        attributes: Additional span attributes
        record_exception: Whether to record exceptions in the span

    Example:
        @trace_function(attributes={"operation": "sentiment_analysis"})
        def analyze_sentiment(text: str) -> float:
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            name = span_name or func.__name__

            with tracer.start_as_current_span(name) as span:
                # Add custom attributes
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                # Add function metadata
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)

                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result

                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def add_span_attributes(attributes: dict[str, Any]) -> None:
    """Add attributes to the current span."""
    span = trace.get_current_span()
    if span:
        for key, value in attributes.items():
            span.set_attribute(key, value)


def add_span_event(name: str, attributes: Optional[dict[str, Any]] = None) -> None:
    """Add an event to the current span."""
    span = trace.get_current_span()
    if span:
        span.add_event(name, attributes=attributes or {})


class SpanContext:
    """Context manager for creating child spans."""

    def __init__(
        self,
        name: str,
        attributes: Optional[dict[str, Any]] = None,
    ):
        self.name = name
        self.attributes = attributes or {}
        self.span: Optional[Span] = None

    def __enter__(self) -> Span:
        tracer = get_tracer()
        self.span = tracer.start_span(self.name)

        for key, value in self.attributes.items():
            self.span.set_attribute(key, value)

        return self.span

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span:
            if exc_type:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            else:
                self.span.set_status(Status(StatusCode.OK))
            self.span.end()
        return False
