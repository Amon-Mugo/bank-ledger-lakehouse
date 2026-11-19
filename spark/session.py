# used as a centralized configuration Settings for Apache Iceberg and Apache Spark
# also builds and sql commands and Hadoop used for local testings
import os
from pathlib import Path
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
# used configure apache iceberg spark runtime jar be compatible with spark,iceberg and scala version
ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
# configuration settings to run sql commands
CATALOG_NAME = "lakehouse"

DEFAULT_WAREHOUSE = Path(
    "data/warehouse"
)  # where iceberg tables and metadata will live


# checks warehouse path
def _resolve_warehouse(warehouse: str | None) -> str:
    if warehouse:
        return warehouse

    return os.environ.get("LAKEHOUSE_WAREHOUSE", str(DEFAULT_WAREHOUSE.resolve()))


# create sparksession with an iceberg hadoop called lakehouse
def build_spark_session(
    app_name: str, warehouse: str | None = None, master: str | None = "local[*]"
) -> SparkSession:

    catalog = f"spark.sql.catalog.{CATALOG_NAME}"
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return (
        builder.config("spark.jars.packages", ICEBERG_PACKAGE)
        # used to set up configuration for cluster management
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        # Set up register iceberg sql extensions
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        # iceberg to use spark catalog
        .config(f"{catalog}.type", "hadoop")
        # file system management is managed by hadoop
        .config(f"{catalog}.warehouse", _resolve_warehouse(warehouse))
        # resolve warehouse path issues
        .config("spark.sql.session.timeZone", "UTC")
        # used to use one unified timezone
        .getOrCreate()
    )
