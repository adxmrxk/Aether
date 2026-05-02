"""
Structured Logging Module

Provides JSON-structured logging with:
- Correlation IDs for request tracing
- Automatic trace context injection
- Google Cloud Logging integration
- Log levels with severity mapping
- Performance-safe lazy evaluation

Follows Google Cloud Logging best practices for filtering and analysis.
"""

import json
import logging
import os
import sys
import traceback
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from opentelemetry import trace

# Context variable for correlation ID
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


class StructuredFormatter(logging.Formatter):
    """
    JSON formatter for structured logging compatible with Google Cloud Logging.

    Output format follows Cloud Logging's special JSON fields:
    - severity: Log level
    - message: Log message
    - timestamp: ISO 8601 timestamp
    - logging.googleapis.com/trace: Trace ID for correlation
    - logging.googleapis.com/spanId: Span ID
    - labels: Custom labels for filtering
    """

    # Map Python log levels to Cloud Logging severity
    SEVERITY_MAP = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def __init__(self, service_name: str = "aether"):
        super().__init__()
        self.service_name = service_name
        self.project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        # Base log entry
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": self.SEVERITY_MAP.get(record.levelno, "DEFAULT"),
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add service metadata
        log_entry["labels"] = {
            "service": self.service_name,
            "environment": os.environ.get("ENVIRONMENT", "development"),
        }

        # Add correlation ID if present
        correlation_id = _correlation_id.get()
        if correlation_id:
            log_entry["correlation_id"] = correlation_id

        # Add OpenTelemetry trace context
        span = trace.get_current_span()
        if span and span.is_recording():
            ctx = span.get_span_context()
            if ctx.is_valid:
                log_entry["logging.googleapis.com/trace"] = (
                    f"projects/{self.project_id}/traces/{format(ctx.trace_id, '032x')}"
                )
                log_entry["logging.googleapis.com/spanId"] = format(ctx.span_id, "016x")
                log_entry["logging.googleapis.com/trace_sampled"] = ctx.trace_flags.sampled

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "stacktrace": traceback.format_exception(*record.exc_info),
            }

        # Add extra fields from record
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in {
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "exc_info", "exc_text", "thread", "threadName",
                "message", "taskName",
            }
        }
        if extra_fields:
            log_entry["context"] = extra_fields

        return json.dumps(log_entry, default=str)


class StructuredLogger:
    """
    Wrapper for structured logging with context management.

    Example:
        logger = StructuredLogger("aether.api")
        logger.info("Processing request", user_id="123", action="search")
    """

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._name = name

    def _log(
        self,
        level: int,
        message: str,
        exc_info: bool = False,
        **kwargs: Any,
    ) -> None:
        """Internal log method with extra context."""
        extra = kwargs
        self.logger.log(level, message, exc_info=exc_info, extra=extra)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, exc_info: bool = True, **kwargs: Any) -> None:
        """Log error message with optional exception info."""
        self._log(logging.ERROR, message, exc_info=exc_info, **kwargs)

    def critical(self, message: str, exc_info: bool = True, **kwargs: Any) -> None:
        """Log critical message."""
        self._log(logging.CRITICAL, message, exc_info=exc_info, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log exception with full traceback."""
        self._log(logging.ERROR, message, exc_info=True, **kwargs)


def init_logging(
    service_name: str = "aether",
    level: int = logging.INFO,
    use_json: bool = True,
) -> None:
    """
    Initialize structured logging for the application.

    Args:
        service_name: Name of the service for log labels
        level: Minimum log level
        use_json: Use JSON formatting (True for production)
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    if use_json:
        handler.setFormatter(StructuredFormatter(service_name))
    else:
        # Development: human-readable format
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        ))

    root_logger.addHandler(handler)


def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name)


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """
    Set correlation ID for the current context.

    Args:
        correlation_id: Custom ID or auto-generate if None

    Returns:
        The correlation ID that was set
    """
    cid = correlation_id or str(uuid4())
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID."""
    return _correlation_id.get()


def clear_correlation_id() -> None:
    """Clear the correlation ID."""
    _correlation_id.set(None)
