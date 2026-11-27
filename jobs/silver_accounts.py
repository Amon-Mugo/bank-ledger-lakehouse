# used as the account iceberg table
import argparse
import logging

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.cleaning import (
    RejectRule,
    _clean_text,
    _is_missing,
    _parse_timestamp,
    is_blank,
    split_valid_and_rejected,
)
from spark.quarantine import write_quarantine
from spark.session import CATALOG_NAME, build_spark_session

logger = logging.getLogger("silver_accounts")
SOURCE_NAME = "accounts"
KEY_COLUMN = "account_id"
SILVER_NAMESPACE = f"{CATALOG_NAME}.silver"
BRONZE_TABLE = f"{CATALOG_NAME}.bronze.{SOURCE_NAME}"
SILVER_TABLE = f"{SILVER_NAMESPACE}.{SOURCE_NAME}"
BRANCHES_TABLE = f"{SILVER_NAMESPACE}.branches"
CUSTOMER_TABLE = f"{SILVER_NAMESPACE}.customers"
BATCH_VIEW = "account_version_batch"

TEST_KEY_PREFIX = "ACTEST"  # synthetic tests accounts
VALID_OPS = ("I", "U", "D")  # mean Insert Update Delete prefixes
DELETE_OP = "D"  # REPRESENTS THE DELETE prefix
REQUIRED_COLUMNS = (
    "customer_id",
    "branch_code",
    "product_type",
    "currency",
    "status",
    "opened_at",
)

ATTRIBUTE_COLUMNS = (
    "customer_id",
    "product_type",
    "branch_code",
    "currency",
    "status",
    "overdraft_limit",
    "opened_at",
    "account_tier",
)

KNOWN_KEY_COLUMNS = (
    "_known_branch_code",
    "_known_customer_id",
)  # Helper columns from the foreign key lookups


# used for cli commands
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Loads bronze accounts into silver SCD2 Iceberg Table"
    )
    parser.add_argument("--warehouse", default=None)
    return parser.parse_args()


# used to build the iceberg table
def _ensure_silver_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        (
            account_id STRING,
            customer_id STRING,
            product_type STRING,
            branch_code STRING,
            currency STRING,
            status STRING,
            overdraft_limit DECIMAL(18,2),
            opened_at TIMESTAMP,
            account_tier STRING,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            is_deleted BOOLEAN,
            source_dt DATE,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        """)


# used as data schema evolution; this is due to account_tier missing in past data
def _with_account_tier(bronze: DataFrame) -> DataFrame:
    if "account_tier" in bronze.columns:
        return bronze  # return current one to avoid overwriting
    return bronze.withColumn("account_tier", F.lit(None).cast("string"))
    # if missing, use NULL and cast as string to match silver schema


# used to check if the foreign keys in accounts exist in silver branches and customers
def _flag_known_keys(spark: SparkSession, df: DataFrame) -> DataFrame:
    branches = (
        spark.table(BRANCHES_TABLE)
        .select(F.col("branch_code").alias("_known_branch_code"))
        .distinct()
    )
    # distinct() deduplicates lookup keys
    customers = (
        spark.table(CUSTOMER_TABLE)
        .select(F.col("customer_id").alias("_known_customer_id"))
        .distinct()
    )

    return df.join(
        F.broadcast(branches),
        F.upper(F.trim(F.col("branch_code"))) == F.col("_known_branch_code"),
        "left",
    ).join(
        customers,
        F.trim(F.col("customer_id")) == F.col("_known_customer_id"),
        "left",
    )


# used to convert string currency values into proper decimal values, e.g. '1,250.00' to 1250.00
def _parse_amount(column_name: str) -> Column:
    return F.expr(
        f"try_cast(regexp_replace(trim(`{column_name}`), ',', '') as decimal(18,2))"
    )
    # accepts up to 16 digits before the decimal point and 2 decimal places after it (18 total digits)


# checks whether op is not 'D' before applying a rule
def _unless_delete(condition: Column) -> Column:
    return (F.upper(F.trim(F.col("op"))) != DELETE_OP) & condition


# builds rejection rules
def _account_rules() -> list[RejectRule]:
    return [
        RejectRule(
            "test_record",
            F.upper(F.trim(F.col(KEY_COLUMN))).startswith(TEST_KEY_PREFIX),
        ),
        RejectRule("_missing_account_id", _is_missing(KEY_COLUMN)),
        RejectRule(
            "invalid_op",
            F.coalesce(~F.upper(F.trim(F.col("op"))).isin(*VALID_OPS), F.lit(True)),
        ),
        RejectRule("invalid_change_ts", _parse_timestamp("change_ts").isNull()),
        *[
            RejectRule(f"_missing_{column}", _unless_delete(_is_missing(column)))
            for column in REQUIRED_COLUMNS
        ],
        RejectRule(
            "invalid_opened_at", _unless_delete(_parse_timestamp("opened_at").isNull())
        ),
        RejectRule(
            "invalid_overdraft_limit",
            _unless_delete(
                ~is_blank("overdraft_limit") & _parse_amount("overdraft_limit").isNull()
            ),
        ),
        RejectRule(
            "orphan_branch_code", _unless_delete(F.col("_known_branch_code").isNull())
        ),
        RejectRule(
            "orphan_customer_id", _unless_delete(F.col("_known_customer_id").isNull())
        ),
    ]


# used to normalise bronze rows into typed silver change_event shape
def _clean_events(valid: DataFrame) -> DataFrame:
    return valid.select(
        F.trim(F.col(KEY_COLUMN)).alias(KEY_COLUMN),
        F.upper(F.trim(F.col("op"))).alias("op"),
        _clean_text("customer_id").alias("customer_id"),
        F.lower(_clean_text("product_type")).alias("product_type"),
        F.upper(_clean_text("branch_code")).alias("branch_code"),
        F.upper(_clean_text("currency")).alias("currency"),
        F.lower(_clean_text("status")).alias("status"),
        _parse_amount("overdraft_limit").alias("overdraft_limit"),
        _parse_timestamp("opened_at").alias("opened_at"),
        F.lower(_clean_text("account_tier")).alias("account_tier"),
        _parse_timestamp("change_ts").alias("change_ts"),
        F.col("dt").cast("date").alias("source_dt"),
        F.col("_ingested_at"),
    )


# used to drop exact duplicate change events, keeping the most recently ingested copy
def _deduplicate_events(events: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy(KEY_COLUMN, "change_ts", "op").orderBy(
        F.col("_ingested_at").desc(), F.col("source_dt").desc()
    )
    return (
        events.withColumn("_rank", F.row_number().over(latest_first))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


# used to arrange the account data from oldest to newest to track any account changes
def _build_versions(events: DataFrame) -> DataFrame:
    chronological = Window.partitionBy(KEY_COLUMN).orderBy("change_ts", "_ingested_at")
    history = chronological.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # enables looking back across prior rows for the same account

    is_delete = F.col("op") == DELETE_OP
    attributes = [
        F.when(is_delete, F.last(column, ignorenulls=True).over(history))
        .otherwise(F.col(column))
        .alias(column)
        for column in ATTRIBUTE_COLUMNS
    ]
    # looks at the next event's change_ts for the same account_id
    next_change = F.lead("change_ts").over(chronological)
    return events.select(
        F.col(KEY_COLUMN),
        *attributes,
        F.col("change_ts").alias(
            "effective_from"
        ),  # timestamp when this account version starts being active
        next_change.alias("effective_to"),
        next_change.isNull().alias("is_current"),
        is_delete.alias("is_deleted"),
        F.col("source_dt"),
        F.col("_ingested_at"),
    )


# used to merge versions on (account_id, effective_from) and update or insert rows
def _merge_versions(spark: SparkSession, versions: DataFrame) -> None:
    versions.createOrReplaceTempView(BATCH_VIEW)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING {BATCH_VIEW} AS source
        ON target.{KEY_COLUMN} = source.{KEY_COLUMN} AND target.effective_from = source.effective_from
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)


# used as the orchestrator
def load_accounts(spark: SparkSession) -> None:
    _ensure_silver_table(spark)  # builds the iceberg table
    bronze = _with_account_tier(spark.table(BRONZE_TABLE))
    valid, rejected = split_valid_and_rejected(
        _flag_known_keys(spark, bronze), _account_rules()
    )
    valid = valid.drop(*KNOWN_KEY_COLUMNS)
    rejected = rejected.drop(*KNOWN_KEY_COLUMNS)
    write_quarantine(spark, rejected, SOURCE_NAME, KEY_COLUMN)
    cleaned = _clean_events(valid)
    events = _deduplicate_events(cleaned)
    # deduplicated data keeping the most recently ingested copy
    _merge_versions(spark, _build_versions(events))
    silver = spark.table(SILVER_TABLE)
    logger.info(
        "Silver %s: bronze=%d, rejected=%d, duplicates_dropped=%d, versions=%d, current=%d, deleted=%d",
        SOURCE_NAME,
        bronze.count(),
        rejected.count(),
        cleaned.count() - events.count(),
        silver.count(),
        silver.filter("is_current").count(),
        silver.filter("is_current AND is_deleted").count(),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = _parse_args()
    spark = build_spark_session("silver_accounts", warehouse=args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    try:
        load_accounts(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
