"""
Centralised configuration for the ETL pipeline.

All volume paths, source formats, schema-validation rules, and known event
types live here so that adding a new event tomorrow only requires updating
this file (or no change at all if the event type is discovered dynamically).
"""


class PipelineConfig:
    """Reusable configuration container for the entire pipeline."""

    # ------------------------------------------------------------------
    # Volume source
    # ------------------------------------------------------------------
    VOLUME_PATH = "/Volumes/cognizant/bronze/source_data/"
    SOURCE_FORMAT = "parquet"

    # ------------------------------------------------------------------
    # Bronze table names
    # ------------------------------------------------------------------
    BRONZE_RAW_EVENTS = "bronze_raw_events"

    # ------------------------------------------------------------------
    # Silver table names
    # ------------------------------------------------------------------
    SILVER_GOOD_RECORDS = "silver_good_records"
    SILVER_BAD_RECORDS = "silver_bad_records"

    # Prefix for event-specific silver tables, e.g. silver_event_order
    SILVER_EVENT_TABLE_PREFIX = "silver_event_"

    # ------------------------------------------------------------------
    # Schema validation – columns that MUST be present and non-null
    # for a record to be classified as a "good" record.
    # Extend this list if new mandatory columns are added upstream.
    # ------------------------------------------------------------------
    REQUIRED_COLUMNS = [
        "event_id",
        "event_type",
        "event_timestamp",
        "payload",
    ]

    # Nested path inside payload that must also be non-null.
    # Adjust if the payload structure changes for new event types.
    REQUIRED_NESTED_COLUMNS = [
        "payload.order",
    ]

    # ------------------------------------------------------------------
    # Known event types in the source data.
    # New event types discovered at runtime are still routed to their own
    # silver tables; this list is used for explicit table definitions and
    # can be extended without touching the rest of the pipeline.
    # ------------------------------------------------------------------
    EVENT_TYPES = [
        "order",
        "purchase",
        "refund",
        "product",
        "shipment",
        "return",
    ]

    # ------------------------------------------------------------------
    # Rescued data column – captures fields that don't match the explicit
    # schema (schema drift, type mismatches, unexpected columns).
    # Records with a non-null _rescued_data value are classified as bad.
    # ------------------------------------------------------------------
    RESCUED_DATA_COLUMN = "_rescued_data"

    # ------------------------------------------------------------------
    # Schema evolution mode for Auto Loader.
    #   addNewColumns       – stream fails, new column appended (default)
    #   rescue              – schema never evolves, drift goes to rescued col
    #   failOnNewColumns    – strict contract enforcement
    #   none                – fixed schema, drift silently dropped
    # We use 'rescue' so schema drift never breaks the pipeline; the
    # _rescued_data column lets the silver layer inspect and route bad data.
    # ------------------------------------------------------------------
    SCHEMA_EVOLUTION_MODE = "rescue"

    # ------------------------------------------------------------------
    # Auto Loader options (format-agnostic).
    # The explicit schema is applied via .schema() in bronze.py, so we
    # disable inference and rely on the schema contract instead.
    # ------------------------------------------------------------------
    AUTOLOADER_OPTIONS = {
        "cloudFiles.format": SOURCE_FORMAT,
        "cloudFiles.schemaEvolutionMode": SCHEMA_EVOLUTION_MODE,
    }
