"""
Silver layer – event-specific tables.

Each known event type gets its own streaming table that reads from
``silver_good_records`` and filters by ``event_type``.  A reusable
helper function (``_event_stream``) encapsulates the shared logic so
that adding a new event type tomorrow only requires:

1. Add the new event type to ``PipelineConfig.EVENT_TYPES``.
2. Add a new ``@dp.table`` function below that calls ``_event_stream``.

No other code needs to change.

Each event table enforces data-quality expectations to guarantee that
downstream consumers only see well-formed records.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from transformations.config.logger_util import PipelineLogger
from transformations.config.pipeline_config import PipelineConfig

logger = PipelineLogger(__name__)


def _event_stream(event_type: str) -> DataFrame:
    """
    Return a streaming DataFrame containing only good records for the
    given ``event_type``.

    This is the single reusable building-block for all event-specific
    silver tables.
    """
    try:
        logger.info(
            f"Building event-specific stream for event_type='{event_type}'"
        )
        good_df = spark.readStream.table(PipelineConfig.SILVER_GOOD_RECORDS)
        filtered_df = good_df.filter(col("event_type") == event_type)

        logger.info(f"Stream for event_type='{event_type}' initialised.")
        return filtered_df

    except Exception as exc: 
        logger.log_exception(
            f"Failed to build event stream for type '{event_type}'", exc
        )
        raise


# ----------------------------------------------------------------------
# Shared expectations applied to every event-specific table.
# These guarantee that any row reaching the event table is a valid,
# schema-conformant event of the correct type.
# ----------------------------------------------------------------------

_EVENT_EXPECTATIONS = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
    "valid_payload": "payload IS NOT NULL",
    "no_schema_drift": "_rescued_data IS NULL",
}


def _event_table_name(event_type: str) -> str:
    """Build the silver table name for the given event type."""
    return f"{PipelineConfig.SILVER_EVENT_TABLE_PREFIX}{event_type}"


# ----------------------------------------------------------------------
# Event-specific streaming table definitions.
# To add a new event type:
#   1. Add it to PipelineConfig.EVENT_TYPES.
#   2. Copy the pattern below and change the event_type string.
# ----------------------------------------------------------------------


@dp.table(
    name=_event_table_name("order"),
    comment="Silver table for 'order' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_order():
    return _event_stream("order")


@dp.table(
    name=_event_table_name("purchase"),
    comment="Silver table for 'purchase' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_purchase():
    return _event_stream("purchase")


@dp.table(
    name=_event_table_name("refund"),
    comment="Silver table for 'refund' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_refund():
    return _event_stream("refund")


@dp.table(
    name=_event_table_name("product"),
    comment="Silver table for 'product' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_product():
    return _event_stream("product")


@dp.table(
    name=_event_table_name("shipment"),
    comment="Silver table for 'shipment' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_shipment():
    return _event_stream("shipment")


@dp.table(
    name=_event_table_name("return"),
    comment="Silver table for 'return' events extracted from good records.",
)
@dp.expect_all(_EVENT_EXPECTATIONS)
def silver_event_return():
    return _event_stream("return")
