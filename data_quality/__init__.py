"""
AetherFlow Data Quality Module
Great Expectations integration for data validation and quality monitoring

Features:
- Automated data quality checks on BigQuery tables
- Custom expectations for market data
- Data docs generation for stakeholder visibility
- Slack/email alerting on validation failures
- Integration with dbt for pipeline validation
"""

from .expectations import (
    AetherDataContext,
    validate_bronze_data,
    validate_market_data,
    validate_sentiment_scores,
)
from .validations import ValidationResult, run_checkpoint

__all__ = [
    "AetherDataContext",
    "validate_bronze_data",
    "validate_sentiment_scores",
    "validate_market_data",
    "run_checkpoint",
    "ValidationResult",
]
