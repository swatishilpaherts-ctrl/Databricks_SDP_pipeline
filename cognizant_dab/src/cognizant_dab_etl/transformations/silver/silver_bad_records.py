"""
Silver layer – bad records.

Reads the raw bronze stream, validates the schema via ``SchemaValidator``
(which checks required columns, nested payload paths, and rescued data),
and writes records that FAIL any check to the ``silver_bad_records`` table.
An extra ``_validation_error`` column describes the first failing check.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, lit

from transformations.config.logger_util import PipelineLogger
from transformations.config.pipeline_config import PipelineConfig
from transformations.config.schema_validator import SchemaValidator

logger = PipelineLogger(__name__)
validator = SchemaValidator()


@dp.table(
    name=PipelineConfig.SILVER_BAD_RECORDS,
    comment="Events that failed schema validation (missing required columns or schema drift detected).",
)
def silver_bad_records():
    """Filter the bronze stream for records that fail schema validation."""
    try:
        logger.info("Reading from bronze raw events stream.")
        raw_df = spark.readStream.table(PipelineConfig.BRONZE_RAW_EVENTS)

        _, bad_df = validator.validate(raw_df)

        # Add a human-readable error description for each failing row
        error_expr = validator.get_validation_reason_expr(bad_df)
        bad_df = bad_df.withColumn("_validation_error", error_expr)

        logger.info("silver_bad_records stream initialised.")
        return bad_df

    except Exception as exc:
        logger.log_exception("Failed to build silver_bad_records stream", exc)
        raise
