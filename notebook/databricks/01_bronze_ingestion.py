# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion
# MAGIC **Goal:** load the raw source files *exactly as received* and add audit columns. No cleaning here.
# MAGIC
# MAGIC | Input | Output |
# MAGIC |---|---|
# MAGIC | 6 raw CSV files (copied by ADF to `bronze/raw_files/<table>/ingest_date=<run_date>/`) | 6 Parquet tables in `bronze/tables/<table>` |
# MAGIC
# MAGIC Audit columns: `_ingestion_timestamp`, `_batch_id`, `_source_system`, `_source_file`, `_row_id`, `_record_hash`.
# MAGIC
# MAGIC > **Status:** written for Databricks / PySpark 3.5. In this delivery the notebooks were *not* executed (PySpark could not be installed in the build environment). The "Expected output" cell shows values produced by the pandas reference implementation of the same logic.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup (identical in every notebook)
# MAGIC * **Azure / Databricks:** parameters arrive from ADF as widgets (`run_date`, `storage_account`, `environment`), data lives in ADLS Gen2 (`abfss://`).
# MAGIC * **Local mode** (`environment = local`, the default when no widget exists): reads `data/raw/*.csv` and writes to `data/*_spark/` so the pandas reference outputs are never overwritten.

# COMMAND ----------
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("logistics_pipeline").getOrCreate()   # `spark` already exists on Databricks
spark.conf.set("spark.sql.ansi.enabled", "false")                        # bad dates / numbers become NULL instead of raising
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")


def get_param(name: str, default: str) -> str:
    """Read an ADF/Databricks widget; fall back to the default when running locally (no dbutils)."""
    try:
        dbutils.widgets.text(name, default)      # noqa: F821  (dbutils only exists on Databricks)
        return dbutils.widgets.get(name)         # noqa: F821
    except NameError:
        return default


ENVIRONMENT = get_param("environment", "local")
STORAGE_ACCOUNT = get_param("storage_account", "<storage-account-name>")
RUN_DATE = get_param("run_date", "2026-06-30")
BATCH_ID = get_param("pipeline_run_id", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
LOCAL_MODE = ENVIRONMENT == "local"
ROOT = os.path.abspath(os.path.join(os.getcwd(), "..")) if os.path.basename(os.getcwd()) in ("notebooks", "databricks_source") else os.getcwd()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("logistics")


def _abfss(container: str, path: str) -> str:
    return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{path}"


def raw_file_path(table: str) -> str:       # raw CSV copied by ADF into bronze/raw_files/<table>/ingest_date=<run_date>/
    return f"{ROOT}/data/raw/{table}.csv" if LOCAL_MODE else _abfss("bronze", f"raw_files/{table}/ingest_date={RUN_DATE}/{table}.csv")


def bronze_path(table: str) -> str:
    return f"{ROOT}/data/bronze_spark/{table}" if LOCAL_MODE else _abfss("bronze", f"tables/{table}")


def silver_path(name: str) -> str:
    return f"{ROOT}/data/processed_spark/{name}" if LOCAL_MODE else _abfss("silver", f"tables/{name}")


def gold_path(name: str) -> str:
    return f"{ROOT}/data/gold_spark/{name}" if LOCAL_MODE else _abfss("gold", name)


def write_table(df: DataFrame, path: str) -> None:
    """Parquet is used so ADF Copy can read the folder directly. (Delta is a drop-in upgrade, see docs.)"""
    df.write.mode("overwrite").parquet(path)


def read_table(path: str) -> DataFrame:
    return spark.read.parquet(path)


def pct(num, den, digits: int = 2):
    """Safe percentage column: NULL when the denominator is 0."""
    return F.when(den > 0, F.round(num / den * 100, digits))

# COMMAND ----------
SOURCES = {"customers": "CRM", "warehouses": "WMS", "inventory": "WMS", "transportation_partners": "ERP",
           "shipments": "TMS", "shipment_tracking_events": "TMS"}


def ingest_table(table: str, source_system: str) -> DataFrame:
    """Read one raw CSV with every column as STRING (inferSchema off) and append the audit columns."""
    df = (spark.read.option("header", True).option("inferSchema", False)
          .option("mode", "PERMISSIVE").csv(raw_file_path(table)))
    business_cols = df.columns
    return (df
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_source_file", F.lit(f"{table}.csv"))          # F.input_file_name() also works on non-UC clusters
            .withColumn("_row_id", F.monotonically_increasing_id())      # preserves file order for 'keep first' de-duplication
            .withColumn("_record_hash", F.md5(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in business_cols]))))

# COMMAND ----------
# Ingest every table; collect failures so one bad file does not hide the others, then fail the job if any failed.
results, failures = [], []
for table, system in SOURCES.items():
    try:
        bronze = ingest_table(table, system)
        write_table(bronze, bronze_path(table))
        n = spark.read.parquet(bronze_path(table)).count()
        results.append((table, system, n))
        log.info("bronze %-26s rows=%d", table, n)
    except Exception as exc:                      # noqa: BLE001
        failures.append((table, str(exc)))
        log.error("bronze %s FAILED: %s", table, exc)

if failures:
    raise RuntimeError(f"Bronze ingestion failed for: {[t for t, _ in failures]}")

# COMMAND ----------
# Sample execution: row counts per Bronze table
spark.createDataFrame(results, ["table", "source_system", "rows"]).show(truncate=False)

# COMMAND ----------
# Sample records incl. audit columns
read_table(bronze_path("shipments")).limit(3).show(truncate=False, vertical=True)
