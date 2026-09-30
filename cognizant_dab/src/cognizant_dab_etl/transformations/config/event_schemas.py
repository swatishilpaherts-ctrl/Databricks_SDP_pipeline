"""
Explicit PySpark StructType schema for the retail events payload.

This module provides a single source of truth for the expected structure
of every event record ingested from the ``source_data`` volume.  When used
with Auto Loader's ``.schema()`` and ``rescuedDataColumn`` option, any field
that does not match this contract is automatically routed to the
``_rescued_data`` column instead of causing a silent type coercion or a
pipeline failure.

Benefits of an explicit schema:
    * Enforces a strict data contract at ingestion time.
    * Captures schema drift (new / renamed / type-changed columns) in
      ``_rescued_data`` for downstream inspection.
    * Eliminates the inference sampling step – Auto Loader starts faster.

To extend the schema when a new field is legitimately added upstream:
    1. Add the field to the appropriate ``StructType`` below.
    2. Re-deploy the pipeline.
    Records that were rescued before the update remain in ``_rescued_data``;
    new records flow into the new column.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------------------------
# Reusable sub-struct definitions
# ---------------------------------------------------------------------------

_ADDRESS_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True),
    StructField("zip_code", StringType(), True),
])

_PREFERENCES_SCHEMA = StructType([
    StructField("newsletter_opted", BooleanType(), True),
    StructField("preferred_category", StringType(), True),
    StructField("preferred_language", StringType(), True),
    StructField("sms_alerts", BooleanType(), True),
])

_PROFILE_SCHEMA = StructType([
    StructField("age_group", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("preferences", _PREFERENCES_SCHEMA, True),
])

_CUSTOMER_SCHEMA = StructType([
    StructField("addresses", ArrayType(_ADDRESS_SCHEMA), True),
    StructField("customer_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("profile", _PROFILE_SCHEMA, True),
    StructField("segment", StringType(), True),
])

_DEVICE_INFO_SCHEMA = StructType([
    StructField("browser", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
])

_METADATA_SCHEMA = StructType([
    StructField("created_by", StringType(), True),
    StructField("device_info", _DEVICE_INFO_SCHEMA, True),
    StructField("ip_address", StringType(), True),
    StructField("source_system", StringType(), True),
])

_CARD_DETAILS_SCHEMA = StructType([
    StructField("card_type", StringType(), True),
    StructField("expiry", StringType(), True),
    StructField("issuer_bank", StringType(), True),
    StructField("last_four", StringType(), True),
])

_PAYMENT_SCHEMA = StructType([
    StructField("amount", DoubleType(), True),
    StructField("card_details", _CARD_DETAILS_SCHEMA, True),
    StructField("currency", StringType(), True),
    StructField("method", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("transaction_ref", StringType(), True),
])

_PRODUCT_ATTRIBUTES_SCHEMA = StructType([
    StructField("brand", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("warranty_months", LongType(), True),
])

_PRODUCT_SCHEMA = StructType([
    StructField("attributes", _PRODUCT_ATTRIBUTES_SCHEMA, True),
    StructField("category", StringType(), True),
    StructField("discount", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("sku", StringType(), True),
    StructField("tax", DoubleType(), True),
    StructField("unit_price", DoubleType(), True),
])

_PROMOTION_SCHEMA = StructType([
    StructField("applied_to", StringType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("promo_code", StringType(), True),
    StructField("promo_id", StringType(), True),
    StructField("promo_type", StringType(), True),
])

_PACKAGE_DIMENSIONS_SCHEMA = StructType([
    StructField("height_cm", LongType(), True),
    StructField("length_cm", LongType(), True),
    StructField("width_cm", LongType(), True),
])

_PACKAGE_SCHEMA = StructType([
    StructField("dimensions", _PACKAGE_DIMENSIONS_SCHEMA, True),
    StructField("items", ArrayType(StringType()), True),
    StructField("package_id", StringType(), True),
    StructField("weight_kg", DoubleType(), True),
])

_SHIPMENT_SCHEMA = StructType([
    StructField("billing_address", _ADDRESS_SCHEMA, True),
    StructField("carrier", StringType(), True),
    StructField("estimated_delivery", StringType(), True),
    StructField("packages", ArrayType(_PACKAGE_SCHEMA), True),
    StructField("ship_date", StringType(), True),
    StructField("shipment_id", StringType(), True),
    StructField("shipping_address", _ADDRESS_SCHEMA, True),
    StructField("status", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("warehouse", StringType(), True),
])

_TOTALS_SCHEMA = StructType([
    StructField("discount_total", DoubleType(), True),
    StructField("grand_total", DoubleType(), True),
    StructField("shipping_cost", DoubleType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("tax_total", DoubleType(), True),
])

_ORDER_SCHEMA = StructType([
    StructField("channel", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("customer", _CUSTOMER_SCHEMA, True),
    StructField("metadata", _METADATA_SCHEMA, True),
    StructField("notes", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payments", ArrayType(_PAYMENT_SCHEMA), True),
    StructField("products", ArrayType(_PRODUCT_SCHEMA), True),
    StructField("promotions", ArrayType(_PROMOTION_SCHEMA), True),
    StructField("shipments", ArrayType(_SHIPMENT_SCHEMA), True),
    StructField("status", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("totals", _TOTALS_SCHEMA, True),
])

_PAYLOAD_SCHEMA = StructType([
    StructField("order", _ORDER_SCHEMA, True),
])


# ---------------------------------------------------------------------------
# Top-level event schema (used by Auto Loader)
# ---------------------------------------------------------------------------

RETAIL_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("payload", PAYLOAD_SCHEMA, True),
    StructField("source", StringType(), True),
    StructField("version", StringType(), True),
    # Rescued data column – captures fields that don't match the schema
    StructField("_rescued_data", StringType(), True),
])


# ---------------------------------------------------------------------------
# DDL string equivalent (useful for SQL-based datasets or debugging)
# ---------------------------------------------------------------------------

RETAIL_EVENT_SCHEMA_DDL = """
    event_id        STRING,
    event_timestamp STRING,
    event_type      STRING,
    payload         STRUCT<
        order: STRUCT<
            channel:       STRING,
            currency:     STRING,
            customer:     STRUCT<
                addresses:     ARRAY<STRUCT<city: STRING, country: STRING, state: STRING, street: STRING, zip_code: STRING>>,
                customer_id:   STRING,
                email:         STRING,
                loyalty_tier:  STRING,
                name:          STRING,
                phone:         STRING,
                profile:       STRUCT<
                    age_group:    STRING,
                    gender:       STRING,
                    preferences:  STRUCT<
                        newsletter_opted:    BOOLEAN,
                        preferred_category: STRING,
                        preferred_language: STRING,
                        sms_alerts:         BOOLEAN
                    >
                >,
                segment:       STRING
            >,
            metadata:      STRUCT<
                created_by:    STRING,
                device_info:   STRUCT<browser: STRING, device_type: STRING, os: STRING>,
                ip_address:    STRING,
                source_system: STRING
            >,
            notes:         STRING,
            order_date:    STRING,
            order_id:      STRING,
            payments:      ARRAY<STRUCT<
                amount:          DOUBLE,
                card_details:    STRUCT<card_type: STRING, expiry: STRING, issuer_bank: STRING, last_four: STRING>,
                currency:        STRING,
                method:          STRING,
                payment_id:      STRING,
                status:          STRING,
                transaction_ref: STRING
            >>,
            products:      ARRAY<STRUCT<
                attributes:  STRUCT<brand: STRING, color: STRING, size: STRING, tags: ARRAY<STRING>, warranty_months: BIGINT>,
                category:    STRING,
                discount:    DOUBLE,
                name:        STRING,
                product_id:  STRING,
                quantity:    BIGINT,
                sku:         STRING,
                tax:         DOUBLE,
                unit_price:  DOUBLE
            >>,
            promotions:    ARRAY<STRUCT<
                applied_to:       STRING,
                discount_amount:  DOUBLE,
                promo_code:       STRING,
                promo_id:         STRING,
                promo_type:       STRING
            >>,
            shipments:     ARRAY<STRUCT<
                billing_address:     STRUCT<city: STRING, country: STRING, state: STRING, street: STRING, zip_code: STRING>,
                carrier:             STRING,
                estimated_delivery:  STRING,
                packages:            ARRAY<STRUCT<
                    dimensions:  STRUCT<height_cm: BIGINT, length_cm: BIGINT, width_cm: BIGINT>,
                    items:       ARRAY<STRING>,
                    package_id:  STRING,
                    weight_kg:   DOUBLE
                >>,
                ship_date:           STRING,
                shipment_id:         STRING,
                shipping_address:   STRUCT<city: STRING, country: STRING, state: STRING, street: STRING, zip_code: STRING>,
                status:              STRING,
                tracking_number:    STRING,
                warehouse:           STRING
            >>,
            status:       STRING,
            store_id:     STRING,
            totals:       STRUCT<
                discount_total: DOUBLE,
                grand_total:    DOUBLE,
                shipping_cost:   DOUBLE,
                subtotal:       DOUBLE,
                tax_total:      DOUBLE
            >
        >
    >,
    source         STRING,
    version        STRING,
    _rescued_data  STRING
"""


class EventSchemaProvider:
    """
    Provides access to the event schema in different formats.

    Usage in bronze layer::

        from transformations.config.event_schemas import EventSchemaProvider

        schema = EventSchemaProvider.get_struct_type()
        ddl  = EventSchemaProvider.get_ddl()
    """

    _struct_type = RETAIL_EVENT_SCHEMA
    _ddl = RETAIL_EVENT_SCHEMA_DDL.strip()

    @classmethod
    def get_struct_type(cls) -> StructType:
        """Return the PySpark ``StructType`` for Auto Loader ``.schema()``."""
        return cls._struct_type

    @classmethod
    def get_ddl(cls) -> str:
        """Return the DDL string equivalent (for SQL datasets or debugging)."""
        return cls._ddl

    @classmethod
    def get_required_columns(cls) -> list:
        """Return the top-level column names (excludes ``_rescued_data``)."""
        return [
            f.name for f in cls._struct_type.fields if f.name != "_rescued_data"
        ]
