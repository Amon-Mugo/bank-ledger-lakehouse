#used in building silver ETL for customer table ONLY
import argparse
import logging
from pyspark.sql import DataFrame, SparkSession, Column, Window
from pyspark.sql import functions as F
from spark.session import CATALOG_NAME, build_spark_session
from spark.cleaning import RejectRule, is_blank, split_valid_and_rejected
from spark.quarantine import write_quarantine

logger = logging.getLogger("silver_customer")

SOURCE_NAME = "customers"
KEY_COLUMNS = "customer_id"
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{SOURCE_NAME}"
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
SILVER_TABLE = f"{SILVER_NAMESPACE}.{SOURCE_NAME}"
BATCH_VIEW = "customers_batch"

TEST_KEY_PREFIX = "CTEST"  # Dummy rows injected by the generator carry this customer_id prefix
VALID_OPS = ("I", "U")  # Customers CDC only inserts and updates; deletes exist on accounts only
MISSING_TOKENS = ("N/A", "NULL")  # Placeholder strings that mean "no value" in the source
REQUIRED_TEXT_COLUMNS = ("full_name", "national_id", "kyc_status")  # Rows missing these are rejected

EAT_OFFSET = "+03:00"  # Source timestamps without a zone are East Africa Time, except the UTC space format
UTC_OFFSET = "+00:00"
ISO_TIMESTAMP_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$"
EPOCH_SECONDS_REGEX = r"^\d{10}$"

# (regex guard, offset appended to the naive value, Spark datetime pattern)
NAIVE_TIMESTAMP_FORMATS = [
    (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", UTC_OFFSET, "yyyy-MM-dd HH:mm:ss XXX"),
    (r"^\d{14}$", EAT_OFFSET, "yyyyMMddHHmmss XXX"),
    (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$", EAT_OFFSET, "dd/MM/yyyy HH:mm:ss XXX"),
    (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M$", EAT_OFFSET, "MM/dd/yyyy hh:mm:ss a XXX"),
]

#used for cli commands
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="loads bronze customers inot silver iceberg tables")
    parser.add_argument("--warehouse", default=None)
    return parser.parse_args()

#used in building the silver iceberg table
def _ensure_silver_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        (
          customer_id STRING,
          full_name  STRING,
          national_id STRING,
          dob   DATE,
          phone STRING,
          email  STRING,
          county  STRING,
          segment STRING,
          kyc_status STRING,
          created_at TIMESTAMP,
          change_ts   TIMESTAMP,
          source_dt   DATE,
          _ingested_at  TIMESTAMP
        )
        USING iceberg
        """
    )

#used to detect null blank and placeholders values such as N/A and NULL
def _is_missing(column_name: str) -> Column:
    return is_blank(column_name) | F.upper(F.trim(F.col(column_name))).isin(*MISSING_TOKENS)

#used to turning missing values into nulls and trim spacess
def _clean_text(column_name: str) -> Column:
    return F.when(_is_missing(column_name), F.lit(None)).otherwise(F.trim(F.col(column_name)))

#fix name spacing ,padding and double spaces
def _clean_name(column_name: str) -> Column:
    return F.initcap(F.regexp_replace(F.trim(F.col(column_name)), r"\s+", " "))

#used to convert timestamps string in multiple formats into single unifide TIMESTAMP
def _parse_timestamp(column_name: str) -> Column:
    raw = F.trim(F.col(column_name))
    iso = F.when(raw.rlike(ISO_TIMESTAMP_REGEX), F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssXXX"))
    epoch = F.when(raw.rlike(EPOCH_SECONDS_REGEX), F.timestamp_seconds(raw.cast("long")))
    naive = [
        F.when(raw.rlike(regex), F.to_timestamp(F.concat_ws(" ", raw, F.lit(offset)), pattern))
        for regex, offset, pattern in NAIVE_TIMESTAMP_FORMATS
    ]
    return F.coalesce(iso, epoch, *naive)


# Used to parse every date of birth format seen in the source; slash dates read as day-first unless the second part is above 12
def _parse_date(column_name: str) -> Column:
    raw = F.trim(F.col(column_name))
    return F.coalesce(
        F.when(raw.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(raw, "yyyy-MM-dd")),
        F.when(raw.rlike(r"^\d{8}$"), F.to_date(raw, "yyyyMMdd")),
        F.when(raw.rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"), F.to_date(raw, "dd-MMM-yyyy")),
        F.when(raw.rlike(r"^\d{2}/(1[3-9]|2\d|3[01])/\d{4}$"), F.to_date(raw, "MM/dd/yyyy")),
        F.when(raw.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(raw, "dd/MM/yyyy")),
    )

# Used to normalize Kenyan phone numbers to +254XXXXXXXXX; anything else becomes NULL
def _normalize_phone(column_name: str) -> Column:
    digits = F.regexp_replace(F.col(column_name), r"\D", "")
    #this is used to delete any non-digt numbers
    return (
        F.when(_is_missing(column_name), F.lit(None))
        .when(digits.rlike(r"^254\d{9}$"), F.concat(F.lit("+"), digits))
        .when(digits.rlike(r"^0\d{9}$"), F.concat(F.lit("+254"), digits.substr(2, 9)))
    )

#used in data quality and quaranitnce bad data
def _customer_rules() -> list[RejectRule]:
    return [
        RejectRule("test_record", F.upper(F.trim(F.col(KEY_COLUMNS))).startswith(TEST_KEY_PREFIX)),
        #used to reject customers starts with CTEST
        RejectRule("_missing_customer_id", _is_missing(KEY_COLUMNS)),
        RejectRule("invalid_op", F.coalesce(~F.trim(F.col("op")).isin(*VALID_OPS), F.lit(True))),
        RejectRule("invalid_change_ts", _parse_timestamp("change_ts").isNull()),
        *[RejectRule(f"_missing_{column}", _is_missing(column)) for column in REQUIRED_TEXT_COLUMNS],
        RejectRule("invalid_dob", _parse_date("dob").isNull()),
        RejectRule("invalid_created_at", _parse_timestamp("created_at").isNull()),
    ]


#used to normalise the bronze record into the silver typed schema
def _clean_customers(valid: DataFrame) -> DataFrame:
    return valid.select(
        F.trim(F.col(KEY_COLUMNS)).alias("customer_id"),
        _clean_name("full_name").alias("full_name"),
        _clean_text("national_id").alias("national_id"),
        _parse_date("dob").alias("dob"),
        _normalize_phone("phone").alias("phone"),
        F.lower(_clean_text("email")).alias("email"),
        _clean_text("county").alias("county"),
        _clean_text("segment").alias("segment"),
        _clean_text("kyc_status").alias("kyc_status"),
        _parse_timestamp("created_at").alias("created_at"),
        _parse_timestamp("change_ts").alias("change_ts"),
        F.col("dt").cast("date").alias("source_dt"),
        F.col("_ingested_at"),
    )

# Used to deduplicate rows so only the single most recent change per customer_id is kept
def _latest_per_key(df: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy(KEY_COLUMNS).orderBy(
        F.col("change_ts").desc(), F.col("_ingested_at").desc(), F.col("source_dt").desc()
    )
    return df.withColumn("_rank", F.row_number().over(latest_first)).filter(F.col("_rank") == 1).drop("_rank")


# Used to merge the latest customer rows into silver; an older change never overwrites a newer silver row
def _merge_into_silver(spark: SparkSession, batch: DataFrame) -> None:
    batch.createOrReplaceTempView(BATCH_VIEW)
    spark.sql(
        f"""
        MERGE INTO {SILVER_TABLE} as target
        USING {BATCH_VIEW} as source
        ON target.{KEY_COLUMNS} = source.{KEY_COLUMNS}
        WHEN MATCHED AND source.change_ts >= target.change_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


#main orcherstartor
def load_customers(spark: SparkSession) -> None:
    _ensure_silver_table(spark)
    bronze = spark.table(BRONZE_TABLE)
    valid, rejected = split_valid_and_rejected(bronze, _customer_rules())
    write_quarantine(spark, rejected, SOURCE_NAME, KEY_COLUMNS)
    latest = _latest_per_key(_clean_customers(valid))
    _merge_into_silver(spark, latest)
    bronze_count = bronze.count()
    rejected_count = rejected.count()
    duplicated_count = valid.count() - latest.count()
    logger.info(
        "Silver %s: bronze=%d,rejected=%d,duplicate_dropped=%d,silver=%d",
        SOURCE_NAME,
        bronze_count,
        rejected_count,
        duplicated_count,
        spark.table(SILVER_TABLE).count(),
    )

#entypoint fot cli
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    spark = build_spark_session("silver_customers", warehouse=args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    try:
        load_customers(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()