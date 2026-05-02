"""
Data Quality Validation Runner

Orchestrates Great Expectations checkpoints for:
- Scheduled validation runs
- Pipeline integration (dbt post-hook)
- CI/CD validation gates
- Alerting on failures
"""

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from google.cloud import bigquery


class ValidationStatus(Enum):
    """Validation result status."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationResult:
    """Result of a validation run."""
    checkpoint_name: str
    status: ValidationStatus
    success_percent: float
    failed_expectations: list[dict[str, Any]]
    run_time: datetime
    table_name: str
    row_count: int
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "checkpoint_name": self.checkpoint_name,
            "status": self.status.value,
            "success_percent": self.success_percent,
            "failed_expectations": self.failed_expectations,
            "run_time": self.run_time.isoformat(),
            "table_name": self.table_name,
            "row_count": self.row_count,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


def run_checkpoint(
    context,
    checkpoint_name: str,
    table_name: str,
    query: Optional[str] = None,
) -> ValidationResult:
    """
    Run a Great Expectations checkpoint.

    Args:
        context: AetherDataContext instance
        checkpoint_name: Name of the checkpoint to run
        table_name: Table being validated
        query: Optional custom query

    Returns:
        ValidationResult with detailed results
    """
    gx_context = context.get_context()
    start_time = datetime.utcnow()

    try:
        # Run checkpoint
        results = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)

        # Parse results
        success_percent = results.statistics.get("success_percent", 0)
        failed = []

        for validation_result in results.list_validation_results():
            for result in validation_result.results:
                if not result.success:
                    failed.append({
                        "expectation_type": result.expectation_config.expectation_type,
                        "kwargs": result.expectation_config.kwargs,
                        "observed_value": result.result.get("observed_value"),
                    })

        status = (
            ValidationStatus.PASSED if results.success
            else ValidationStatus.FAILED
        )

        return ValidationResult(
            checkpoint_name=checkpoint_name,
            status=status,
            success_percent=success_percent,
            failed_expectations=failed,
            run_time=start_time,
            table_name=table_name,
            row_count=results.statistics.get("evaluated_expectations", 0),
            metadata={
                "run_id": str(results.run_id),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
            },
        )

    except Exception as e:
        return ValidationResult(
            checkpoint_name=checkpoint_name,
            status=ValidationStatus.ERROR,
            success_percent=0,
            failed_expectations=[{"error": str(e)}],
            run_time=start_time,
            table_name=table_name,
            row_count=0,
            metadata={"error": str(e)},
        )


def log_validation_to_bigquery(
    result: ValidationResult,
    project_id: str,
    dataset_id: str = "aether_lakehouse",
    table_id: str = "data_quality_log",
) -> None:
    """
    Log validation results to BigQuery for tracking and alerting.

    Args:
        result: ValidationResult to log
        project_id: GCP project ID
        dataset_id: BigQuery dataset
        table_id: Table for logging
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    rows = [{
        "checkpoint_name": result.checkpoint_name,
        "status": result.status.value,
        "success_percent": result.success_percent,
        "failed_count": len(result.failed_expectations),
        "failed_expectations": json.dumps(result.failed_expectations),
        "run_time": result.run_time.isoformat(),
        "table_name": result.table_name,
        "row_count": result.row_count,
        "metadata": json.dumps(result.metadata),
    }]

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        raise RuntimeError(f"Failed to log validation results: {errors}")
