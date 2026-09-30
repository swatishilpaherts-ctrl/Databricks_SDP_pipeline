"""
Bronze layer – raw ingestion from the source_data volume via Auto Loader.

This module reads parquet files from the UC volume ``cognizant.bronze.source_data``
and creates a single streaming table ``bronze_raw_events``.  No transformation
or filtering happens here – the goal is to land every record as-is so the
silver layer can validate and segregate.
"""

from pyspark import pipelines as dp

from transformations.config.event_schemas import EventSchemaProvider
from transformations.config.logger_util import PipelineLogger
from transformations.config.pipeline_config import PipelineConfig

logger = PipelineLogger(__name__)


@dp.table(
    name=PipelineConfig.BRONZE_RAW_EVENTS,
    comment="Raw events ingested from the source_data volume via Auto Loader with explicit schema and rescued data column.",
)
def bronze_raw_events():
    """
    Stream parquet files from the volume using Auto Loader.

    An explicit ``StructType`` schema (from ``EventSchemaProvider``) is applied
    so that Auto Loader validates the payload structure during read.  Any field
    that doesn't match the schema contract is routed to the ``_rescued_data``
    column instead of causing a pipeline failure.
    """
    try:
        logger.info(
            "Starting Auto Loader ingestion",
            context={
                "path": PipelineConfig.VOLUME_PATH,
                "format": PipelineConfig.SOURCE_FORMAT,
                "schema_evolution": PipelineConfig.SCHEMA_EVOLUTION_MODE,
            },
        )

        # Obtain the explicit schema from the schema provider
        event_schema = EventSchemaProvider.get_struct_type()

        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", PipelineConfig.SOURCE_FORMAT)
            .option("cloudFiles.schemaEvolutionMode", PipelineConfig.SCHEMA_EVOLUTION_MODE)
            .option("rescuedDataColumn", PipelineConfig.RESCUED_DATA_COLUMN)
            .schema(event_schema)
        )

        df = reader.load(PipelineConfig.VOLUME_PATH)

        logger.info(
            "Auto Loader stream initialised with explicit schema.",
            context={"rescued_column": PipelineConfig.RESCUED_DATA_COLUMN},
        )
        return df

    except Exception as exc:
        logger.log_exception("Failed to initialise bronze Auto Loader stream", exc)
        raise
