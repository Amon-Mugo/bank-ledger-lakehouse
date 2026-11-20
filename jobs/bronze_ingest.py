# bronze ingestion layer for medallion architecture using spark and iceberg it reads data from
# muiltiple sources and formats (CSV,jsonl,parquet) orgineizes by partiotion and normalises raw business columns

import argparse  # command line arguments
from pathlib import Path
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql import functions as F
from spark.session import CATALOG_NAME, build_spark_session
import logging

logger = logging.getLogger("bronze_ingest")

BRONZE_NAMESPACE = f"{CATALOG_NAME}.bronze"
DEFAULT_SOURCE_DIR = Path("data/out_dev")

SOURCE_FORMATS = {
    "branches": "csv",
    "customers": "csv",
    "accounts": "csv",
    "transactions": "jsonl",
    "loans": "parquet",
    "loan_repayments": "parquet",
}


# used to build CLI arguments commands
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw source files into bronze iceberg tables"
    )
    # used to initiate the cli arguments

    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--warehouse", default=None)
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(SOURCE_FORMATS),
        default=list(SOURCE_FORMATS),
    )
    return parser.parse_args()


# used in reading and casting raw partitions
def _read_partition(spark: SparkSession, fmt: str, path: Path) -> DataFrame:

    if fmt == "csv":
        df = spark.read.option("header", "true").csv(str(path))
    elif fmt == "jsonl":
        df = spark.read.option("primitivesAsString", "true").json(str(path))

    elif fmt == "parquet":
        df = spark.read.parquet(str(path))
    else:
        raise ValueError(f"Unsupported source format:{fmt}")
    return df.select(
        [F.col(f"`{name}`").cast("string").alias(name) for name in df.columns]
    )


# adds metadata and partitioning
def _with_metadata(df: DataFrame, dt: str) -> DataFrame:
    return (
        df.withColumn(
            "_source_file", F.input_file_name()
        )  # where each row data come from
        .withColumn("_ingested_at", F.current_timestamp())  # wheb did the data arrive
        .withColumn("dt", F.lit(dt).cast("date"))  # partition dt to store date
    )


# adds existing data  and compare incoming data and adds missing columns
def _evolve_table(spark: SparkSession, table: str, df: DataFrame) -> None:
    exsisting = {field.name for field in spark.table(table).schema.fields}
    # enables us to know the incoming data names in the columns

    new_columns = [name for name in df.columns if name not in exsisting]
    # to know the icoming new data
    if not new_columns:
        return
    ddl = ", ".join(f"`{name}` string" for name in new_columns)
    # enables us to update table schema and to accept new incoming data
    spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({ddl})")
    # used for executing DDL  statemnt to add new columns in the table
    logger.info("Schema evolved %s added %s", table, new_columns)


# used in matchig the column order and data types of the target table and nulls are non-esisting column data
def _table_columns(spark: SparkSession, table: str, df: DataFrame) -> list[Column]:
    return [
        (
            F.col(f"`{field.name}`")
            if field.name in df.columns
            else F.lit(None).cast(field.dataType).alias(field.name)
        )
        for field in spark.table(table).schema.fields
    ]


# used write a single prepared partition dataframe into target Bronze Iceberg safely
def _write_partition(spark: SparkSession, table: str, df: DataFrame) -> None:
    if not spark.catalog.tableExists(table):
        df.writeTo(table).partitionedBy(F.col("dt")).create()
        return  # exits
    _evolve_table(spark, table, df)
    df.select(_table_columns(spark, table, df)).writeTo(table).overwritePartitions()


# used to orchestrate all data partitions for a single source table form row source into the Bronze Iceberg table
# extratcs date stings and reads and row stings
def ingestion_table(
    spark: SparkSession, source_dir: Path, table: str, fmt: str
) -> None:

    partitions = sorted((source_dir / table).glob("dt=*"))
    # builds a path for the the data neeeded and looks for the dt forlders
    for partition in partitions:
        dt = partition.name.split("=", 1)[1]
        # extracts only the date form dt folder
        df = _with_metadata(_read_partition(spark, fmt, partition), dt)
        _write_partition(spark, f"{BRONZE_NAMESPACE}.{table}", df)
    logger.info("Ingested %d partitions into bronze %s ", len(partitions), table)


# enrtypoint for the Bronze Ingestion jobs
def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args()
    spark = build_spark_session("bronze_ingest", warehouse=args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")
    for table in args.tables:
        ingestion_table(spark, args.source_dir, table, SOURCE_FORMATS[table])
        # lets all spark session process all request table in squence instead separete

    spark.stop()


if __name__ == "__main__":
    main()
