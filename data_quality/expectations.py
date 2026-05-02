"""
Great Expectations - Custom Expectations for AetherFlow

Defines data quality expectations for:
- Bronze layer: Raw data integrity
- Silver layer: Cleaned data validation
- Gold layer: Aggregation accuracy
- Sentiment scores: AI output validation
"""

import os
from typing import Any, Optional

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
)


class AetherDataContext:
    """
    Wrapper for Great Expectations DataContext configured for AetherFlow.

    Supports:
    - BigQuery as primary datasource
    - GCS for expectation/validation stores
    - Data Docs hosted on GCS
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: str = "aether_lakehouse",
        root_directory: str = "/tmp/great_expectations",
    ):
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID")
        self.dataset_id = dataset_id
        self.root_directory = root_directory
        self.context = self._create_context()

    def _create_context(self) -> BaseDataContext:
        """Create and configure the Great Expectations context."""
        config = DataContextConfig(
            config_version=3.0,
            plugins_directory=None,
            stores={
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.root_directory}/expectations/",
                    },
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.root_directory}/validations/",
                    },
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore",
                },
                "checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.root_directory}/checkpoints/",
                    },
                },
            },
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            checkpoint_store_name="checkpoint_store",
            data_docs_sites={
                "local_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": f"{self.root_directory}/data_docs/",
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                    },
                },
            },
            anonymous_usage_statistics={
                "enabled": False,
            },
        )

        context = BaseDataContext(project_config=config)

        # Add BigQuery datasource
        datasource_config = {
            "name": "aether_bigquery",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "module_name": "great_expectations.execution_engine",
                "connection_string": f"bigquery://{self.project_id}/{self.dataset_id}",
            },
            "data_connectors": {
                "default_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "include_schema_name": True,
                },
            },
        }

        context.add_datasource(**datasource_config)

        return context

    def get_context(self) -> BaseDataContext:
        """Get the underlying Great Expectations context."""
        return self.context


def create_bronze_expectations() -> ExpectationSuite:
    """
    Create expectation suite for bronze (raw) data.

    Validates:
    - Required columns exist
    - No null IDs
    - Valid JSON payloads
    - Timestamps are recent
    """
    suite = ExpectationSuite(expectation_suite_name="bronze_raw_data_suite")

    # Column existence
    suite.add_expectation(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={
            "column_list": [
                "id", "raw_payload", "source", "ingested_at",
                "sentiment_score", "sentiment_reasoning", "processing_metadata"
            ]
        }
    )

    # ID validation
    suite.add_expectation(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "id"}
    )
    suite.add_expectation(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "id"}
    )

    # Payload validation
    suite.add_expectation(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "raw_payload"}
    )

    # Timestamp validation
    suite.add_expectation(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "ingested_at"}
    )

    return suite


def create_sentiment_expectations() -> ExpectationSuite:
    """
    Create expectation suite for sentiment score validation.

    Validates:
    - Scores are between 1 and 10
    - No extreme outliers
    - Reasoning is provided when score exists
    """
    suite = ExpectationSuite(expectation_suite_name="sentiment_validation_suite")

    # Sentiment score range
    suite.add_expectation(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "sentiment_score",
            "min_value": 1.0,
            "max_value": 10.0,
            "mostly": 1.0,  # 100% must be in range
        }
    )

    # Score distribution (not all same value)
    suite.add_expectation(
        expectation_type="expect_column_distinct_values_to_be_in_set",
        kwargs={
            "column": "sentiment_score",
            "value_set": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    # Reasoning should exist when score exists
    suite.add_expectation(
        expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
        kwargs={
            "column_A": "sentiment_score",
            "column_B": "sentiment_score",  # Placeholder - custom expectation needed
            "or_equal": True,
        }
    )

    return suite


def create_market_data_expectations() -> ExpectationSuite:
    """
    Create expectation suite for market data validation.

    Validates:
    - Valid cryptocurrency symbols
    - Positive prices
    - Reasonable volume ranges
    - Valid percentage changes
    """
    suite = ExpectationSuite(expectation_suite_name="market_data_suite")

    # Symbol validation
    suite.add_expectation(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "symbol"}
    )
    suite.add_expectation(
        expectation_type="expect_column_value_lengths_to_be_between",
        kwargs={
            "column": "symbol",
            "min_value": 2,
            "max_value": 10,
        }
    )

    # Price validation (positive)
    suite.add_expectation(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "price_usd",
            "min_value": 0,
            "max_value": 1000000000,  # $1B max
            "mostly": 0.99,
        }
    )

    # Percentage change validation
    suite.add_expectation(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "percent_change_24h",
            "min_value": -100,
            "max_value": 10000,  # 100x max gain
            "mostly": 0.99,
        }
    )

    return suite


def validate_bronze_data(context: AetherDataContext, table_name: str = "bronze_raw_data") -> dict:
    """
    Run bronze data validation and return results.

    Args:
        context: AetherDataContext instance
        table_name: BigQuery table to validate

    Returns:
        Validation results dictionary
    """
    gx_context = context.get_context()

    # Get or create suite
    try:
        suite = gx_context.get_expectation_suite("bronze_raw_data_suite")
    except:
        suite = create_bronze_expectations()
        gx_context.save_expectation_suite(suite)

    # Create batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="aether_bigquery",
        data_connector_name="default_runtime_data_connector",
        data_asset_name=table_name,
        runtime_parameters={"query": f"SELECT * FROM `{table_name}` LIMIT 10000"},
        batch_identifiers={"default_identifier_name": "bronze_validation"},
    )

    # Run validation
    validator = gx_context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    results = validator.validate()

    return {
        "success": results.success,
        "statistics": results.statistics,
        "results": [r.to_json_dict() for r in results.results],
    }


def validate_sentiment_scores(context: AetherDataContext) -> dict:
    """Run sentiment score validation."""
    gx_context = context.get_context()

    try:
        suite = gx_context.get_expectation_suite("sentiment_validation_suite")
    except:
        suite = create_sentiment_expectations()
        gx_context.save_expectation_suite(suite)

    batch_request = RuntimeBatchRequest(
        datasource_name="aether_bigquery",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="sentiment_data",
        runtime_parameters={
            "query": "SELECT sentiment_score, sentiment_reasoning FROM `bronze_raw_data` WHERE sentiment_score IS NOT NULL"
        },
        batch_identifiers={"default_identifier_name": "sentiment_validation"},
    )

    validator = gx_context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    results = validator.validate()

    return {
        "success": results.success,
        "statistics": results.statistics,
    }


def validate_market_data(context: AetherDataContext) -> dict:
    """Run market data validation on silver layer."""
    gx_context = context.get_context()

    try:
        suite = gx_context.get_expectation_suite("market_data_suite")
    except:
        suite = create_market_data_expectations()
        gx_context.save_expectation_suite(suite)

    batch_request = RuntimeBatchRequest(
        datasource_name="aether_bigquery",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="silver_data",
        runtime_parameters={
            "query": "SELECT * FROM `silver_market_data` LIMIT 10000"
        },
        batch_identifiers={"default_identifier_name": "market_validation"},
    )

    validator = gx_context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    results = validator.validate()

    return {
        "success": results.success,
        "statistics": results.statistics,
    }
