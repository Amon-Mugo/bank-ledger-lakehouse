# used as the SILVER LAYER this separates bad data rather than drop the data

from dataclasses import dataclass
from typing import List, Tuple, Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

REJECT_REASON_COL = "_reject_reason"


@dataclass(frozen=True)
class RejectRule:
    reason: str
    is_bad: Column


# used in checking any null values and removes any white spaces
def is_blank(column_name: str) -> Column:
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")


# used to make any missing values NULL and tirm any spaces of columns that are not null
def blank_to_null(column_name: str) -> Column:
    return F.when(is_blank(column_name), F.lit(None)).otherwise(F.trim(F.col(column_name)))


# used in casting proper datatypes to columns as well as empty columns to nulls
def safe_cast(column_name: str, data_type: str) -> Column:
    return F.expr(f"try_cast(trim(`{column_name}`)as {data_type})")


# used to reject records that are null or blank
def required_rule(column_name: str, reason: Optional[str] = None) -> RejectRule:
    return RejectRule(reason or f"_missing_{column_name}", is_blank(column_name))


# rejects records where  cannot be cast
def unparseable_rule(column_name: str, data_type: str, reason: Optional[str] = None) -> RejectRule:
    is_bad = ~is_blank(column_name) & safe_cast(column_name, data_type).isNull()
    return RejectRule(reason or f"invalid_{column_name}", is_bad)


# checks which was the first failure of the rule
def attach_reject_reason(df: DataFrame, rules: list[RejectRule]) -> DataFrame:
    first_failure = F.coalesce(*[F.when(rule.is_bad, F.lit(rule.reason)) for rule in rules])
    # ckecks the first rule of the failure
    return df.withColumn(REJECT_REASON_COL, first_failure)


# this returns both valid and rejected dataframes
def split_valid_and_rejected(df: DataFrame, rules: List[RejectRule]) -> Tuple[DataFrame, DataFrame]:
    tagged = attach_reject_reason(df, rules)  # taged data with valid and rejetced data with _is_rejected tag
    valid = tagged.filter(F.col(REJECT_REASON_COL).isNull()).drop(REJECT_REASON_COL)
    rejected = tagged.filter(F.col(REJECT_REASON_COL).isNotNull())
    return valid, rejected
