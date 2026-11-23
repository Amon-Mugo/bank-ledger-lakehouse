# used to hold the bad data into a single table called quarantined.rejected_records

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark.cleaning import REJECT_REASON_COL

QUARANTINE_NAMESPACE = "lakehouse.quarantine"  # used to hold the name of the quarantine namespace

QUARANTINE_TABLE = f"{QUARANTINE_NAMESPACE}.rejected_records"  # table name

QUARANTINE_COLUMNS = [
    "source_table",  # table name of the bad data
    "dt",  # group, filter, and overwrite bad records by date
    "source_file",  # where the rejected file originated from the file path
    "entity_key",  # used to hold the bad row eg order_id="900" which specific record had a problem
    "reject_reason",  # why the record was rejected
    "raw_record",  # holds the original bad data as JSON
    "rejected_at",  # timestamp when the data was quarantined
]  # {"order_id":104,"amount":-25.0,"currency":"USD","dt":"2026-11-23","_source_file":"s3://bucket/orders.csv"}


# used to create the quarantined table if it does not exist
def ensure_quarantine_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {QUARANTINE_NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            source_table STRING,
            dt STRING,
            source_file STRING,
            entity_key STRING,
            reject_reason STRING,
            raw_record STRING,
            rejected_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_table, dt)
        """)


# used to build the quarantine dataframe
def build_quarantine_frame(rejected: DataFrame, source_table: str, key_column: str) -> DataFrame:
    # key_column is the column identifying the record
    payload_columns = [name for name in rejected.columns if name != REJECT_REASON_COL]
    return rejected.select(
        F.lit(source_table).alias("source_table"),
        F.col("dt").cast("string").alias("dt"),
        F.col("_source_file").alias("source_file"),
        F.col(key_column).cast("string").alias("entity_key"),
        F.col(REJECT_REASON_COL).alias("reject_reason"),
        F.to_json(F.struct(*payload_columns)).alias("raw_record"),
        F.current_timestamp().alias("rejected_at"),
    ).select(*QUARANTINE_COLUMNS)


# used to write the quarantine into iceberg quarantine
def write_quarantine(spark: SparkSession, rejected: DataFrame, source_table: str, key_column: str) -> None:
    ensure_quarantine_table(spark)  # create lakehouse.quarantine and lakehouse.quarantine.rejected_records if they do not already exist

    quarantine_frame = build_quarantine_frame(rejected, source_table, key_column)
    # builds the standardized quarantine DataFrame
    quarantine_frame.writeTo(QUARANTINE_TABLE).overwritePartitions()  # used to write to the iceberg table and replace touched partitions on reruns
