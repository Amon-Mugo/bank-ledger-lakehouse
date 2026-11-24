# Used in building the silver ETL for BRANCHES table  ONLY which is used to read bronze.branches, reject records that are null or blank
# to quarantine, and trims/cleans the rest. They are written to silver.branches with MERGE so rerunning jobs doesn't duplicate rows.

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.cleaning import RejectRule, blank_to_null, required_rule, split_valid_and_rejected
from spark.quarantine import write_quarantine
from spark.session import CATALOG_NAME, build_spark_session

logger = logging.getLogger("silver_branches")

SOURCE_NAME = "branches"  # Used to build table name and log messages
KEY_COLUMN = "branch_code"  # Column used to uniquely identify branches, e.g. branch_code = BR001
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{SOURCE_NAME}"  # Iceberg identifier name for bronze table
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
SILVER_TABLE = f"{SILVER_NAMESPACE}.{SOURCE_NAME}"
BATCH_VIEW = "branches_batch"


# Used for CLI commands
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Loads bronze branches into silver iceberg tables")
    parser.add_argument("--warehouse", default=None)
    return parser.parse_args()


# Used to build the silver iceberg table
def _ensure_silver_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            branch_code STRING,
            name STRING,
            county STRING,
            source_dt DATE,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        """)


# Validation rules
def _branch_rules() -> list[RejectRule]:
    return [required_rule("branch_code"), required_rule("name")]


# Used to normalize clean bronze records into the clean silver schema
def _clean_branches(valid: DataFrame) -> DataFrame:
    return valid.select(
        blank_to_null("branch_code").alias("branch_code"),
        blank_to_null("name").alias("name"),
        blank_to_null("county").alias("county"),
        F.col("dt").alias("source_dt"),
        F.col("_ingested_at"),
    )


# Used to deduplicate rows so only the single most recent record per branch_code is kept
def _latest_per_key(df: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy(KEY_COLUMN).orderBy(F.col("source_dt").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rank", F.row_number().over(latest_first)).filter(F.col("_rank") == 1).drop("_rank")


# Used to merge deduplicated branch rows into silver using branch_code as the lookup key
def _merge_into_silver(spark: SparkSession, batch: DataFrame) -> None:
    batch.createOrReplaceTempView(BATCH_VIEW)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING {BATCH_VIEW} AS source
        ON target.{KEY_COLUMN} = source.{KEY_COLUMN}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)


# Main orchestration function for the silver branches load
def load_branches(spark: SparkSession) -> None:
    _ensure_silver_table(spark)  # Builds the silver table if it doesn't exist
    bronze = spark.table(BRONZE_TABLE)  # Loads the bronze iceberg table into a DataFrame
    valid, rejected = split_valid_and_rejected(bronze, _branch_rules())  # Splits rows based on the rules
    write_quarantine(spark, rejected, SOURCE_NAME, KEY_COLUMN)
    latest = _latest_per_key(_clean_branches(valid))
    _merge_into_silver(spark, latest)
    bronze_count = bronze.count()
    rejected_count = rejected.count()
    duplicate_count = valid.count() - latest.count()
    logger.info(
        "Silver %s: bronze=%d rejected=%d duplicates_dropped=%d silver=%d",
        SOURCE_NAME,
        bronze_count,
        rejected_count,
        duplicate_count,
        spark.table(SILVER_TABLE).count(),
    )


# Entrypoint for the CLI and Spark
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    spark = build_spark_session("silver_branches", warehouse=args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    try:
        load_branches(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
