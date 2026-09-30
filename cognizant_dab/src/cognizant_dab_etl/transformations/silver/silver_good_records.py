"""
Silver layer – good records.

Reads the raw bronze stream, validates the schema via ``SchemaValidator``
(which checks required columns, nested payload paths, and rescued data),
and writes records that PASS all checks to the ``silver_good_records`` table.

Data quality expectations are enforced so that only clean records propagate
downstream to the event-specific silver tables.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col

from transformations.config.logger_util import PipelineLogger
from transformations.config.pipeline_config import PipelineConfig
from transformations.config.schema_validator import SchemaValidator

logger = PipelineLogger(__name__)
validator = SchemaValidator()


@dp.table(
    name=PipelineConfig.SILVER_GOOD_RECORDS,
    comment="Events that passed schema validation (required columns non-null, no rescued data).",
)
@dp.expect_all({
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
    "valid_payload": "payload IS NOT NULL",
    "no_schema_drift": "_rescued_data IS NULL",
})
def silver_good_records():
    """Filter the bronze stream for records that pass schema validation."""
    try:
        logger.info("Reading from bronze raw events stream.")
        raw_df = spark.readStream.table(PipelineConfig.BRONZE_RAW_EVENTS)

        good_df, _ = validator.validate(raw_df)

        logger.info("silver_good_records stream initialised.")
        return good_df

    except Exception as exc:
        logger.log_exception("Failed to build silver_good_records stream", exc)
        raise
