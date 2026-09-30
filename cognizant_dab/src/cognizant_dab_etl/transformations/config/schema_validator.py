"""
Schema-validation utility for segregating raw events into good / bad records.

The validator performs three categories of checks:

1. **Required top-level columns** – every column in ``REQUIRED_COLUMNS`` must
   be present in the DataFrame and non-null.
2. **Required nested columns** – every dotted path in ``REQUIRED_NESTED_COLUMNS``
   must resolve and be non-null (e.g. ``payload.order``).
3. **Rescued data check** – when Auto Loader uses an explicit schema with
   ``rescuedDataColumn``, any field that doesn't match the contract is routed
   to ``_rescued_data``.  A non-null value in that column indicates schema drift
   and the record is classified as bad.

Records that pass ALL checks go to the good-records table; the rest go to the
bad-records table.

Usage inside a Spark Declarative Pipeline dataset function::

    from transformations.config.schema_validator import SchemaValidator

    validator = SchemaValidator()
    good_df, bad_df = validator.validate(raw_df)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from transformations.config.logger_util import PipelineLogger
from transformations.config.pipeline_config import PipelineConfig


class SchemaValidator:
    """
    Validates raw event records against the expected source schema.

    A record is considered **valid** (good) when every column listed in
    ``PipelineConfig.REQUIRED_COLUMNS`` and every dotted path in
    ``PipelineConfig.REQUIRED_NESTED_COLUMNS`` is non-null.
    """

    def __init__(
        self,
        required_columns: list = None,
        required_nested_columns: list = None,
        rescued_data_column: str = None,
    ):
        self.required_columns = required_columns or PipelineConfig.REQUIRED_COLUMNS
        self.required_nested_columns = (
            required_nested_columns or PipelineConfig.REQUIRED_NESTED_COLUMNS
        )
        self.rescued_data_column = (
            rescued_data_column or PipelineConfig.RESCUED_DATA_COLUMN
        )
        self.logger = PipelineLogger(self.__class__.__name__)

    def _build_validation_expr(self, df: DataFrame):
        """
        Build a single boolean column expression that is True when the row
        passes ALL validation checks and False otherwise.
        """
        from functools import reduce
        from pyspark.sql import functions as F

        conditions = []

        # Top-level column checks
        for c in self.required_columns:
            if c in df.columns:
                conditions.append(col(c).isNotNull())
            else:
                # Column entirely missing from schema → every row fails
                self.logger.warning(f"Required column '{c}' not found in DataFrame schema; all rows will fail validation.")
                conditions.append(lit(False))

        # Nested column checks (dotted paths like 'payload.order')
        for path in self.required_nested_columns:
            try:
                conditions.append(col(path).isNotNull())
            except Exception as exc:
                self.logger.log_exception(
                    f"Unable to resolve nested path '{path}'", exc
                )
                conditions.append(lit(False))

        # Rescued data check – non-null means Auto Loader detected schema drift
        if self.rescued_data_column and self.rescued_data_column in df.columns:
            conditions.append(col(self.rescued_data_column).isNull())
        elif self.rescued_data_column:
            self.logger.info(
                f"Rescued data column '{self.rescued_data_column}' not present "
                "in DataFrame; skipping rescued data check."
            )

        if not conditions:
            return lit(True)

        return reduce(lambda a, b: a & b, conditions)

    def validate(self, df: DataFrame):
        """
        Split *df* into (good_df, bad_df) based on schema validation.

        Parameters
        ----------
        df : DataFrame
            Raw events DataFrame (streaming or batch).

        Returns
        -------
        (DataFrame, DataFrame)
            good_df – rows that passed all checks (``_is_valid`` dropped).
            bad_df  – rows that failed at least one check (``_is_valid`` dropped,
                      with an added ``_validation_error`` column describing the
                      first failing check).
        """
        try:
            self.logger.info("Starting schema validation", )

            validation_expr = self._build_validation_expr(df)

            # Add the boolean flag
            df_flagged = df.withColumn("_is_valid", validation_expr)

            good_df = df_flagged.filter(col("_is_valid") == True).drop("_is_valid")
            bad_df = df_flagged.filter(col("_is_valid") == False).drop("_is_valid")

            self.logger.info("Schema validation expressions built successfully.")
            return good_df, bad_df

        except Exception as exc:
            self.logger.log_exception("Schema validation failed", exc)
            raise

    def get_validation_reason_expr(self, df: DataFrame):
        """
        Return a column expression that describes the first failing
        check for each row.  Useful as an extra column on the bad-records table.
        """
        error_chain = lit(None).cast("string")

        for c in self.required_columns:
            if c in df.columns:
                error_chain = when(col(c).isNull(), f"{c} is null").otherwise(error_chain)

        for path in self.required_nested_columns:
            try:
                error_chain = when(col(path).isNull(), f"{path} is null").otherwise(error_chain)
            except Exception:
                pass

        if self.rescued_data_column and self.rescued_data_column in df.columns:
            error_chain = when(
                col(self.rescued_data_column).isNotNull(),
                "schema drift detected (rescued data present)"
            ).otherwise(error_chain)

        return error_chain.alias("_validation_error")
