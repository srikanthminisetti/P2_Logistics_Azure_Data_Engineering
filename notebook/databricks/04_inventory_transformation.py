# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Inventory transformation
# MAGIC **Goal:** classify every SKU position and add trend information.
# MAGIC
# MAGIC | Rule | Definition |
# MAGIC |---|---|
# MAGIC | Critical | `quantity_on_hand <= 50% of reorder_level` |
# MAGIC | Low | `quantity_on_hand <= reorder_level` |
# MAGIC | Overstock | `quantity_on_hand >= 3 x reorder_level` |
# MAGIC | Healthy | everything else |
# MAGIC
# MAGIC Techniques: join to warehouses, `lag()` window (previous month), `max()` window (latest snapshot per warehouse).
# MAGIC
# MAGIC > **Status:** not executed in this delivery (PySpark unavailable in the build environment) - see "Expected output".

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
def stock_status(qty, reorder):
    return (F.when(qty <= reorder * 0.5, "Critical").when(qty <= reorder, "Low")
             .when(qty >= reorder * 3.0, "Overstock").otherwise("Healthy"))

# COMMAND ----------
try:
    inv = read_table(silver_path("inventory"))
    wh = read_table(silver_path("warehouses")).select("warehouse_id", "warehouse_name", "city", "region", "capacity_units")

    by_sku = Window.partitionBy("warehouse_id", "sku").orderBy("snapshot_date")
    by_wh = Window.partitionBy("warehouse_id")

    enriched = (inv.join(F.broadcast(wh), "warehouse_id", "left")
                   .withColumn("stock_value", F.round(F.col("quantity_on_hand") * F.col("unit_cost"), 2))
                   .withColumn("stock_status", stock_status(F.col("quantity_on_hand"), F.col("reorder_level")))
                   .withColumn("is_low_stock", F.col("stock_status").isin("Critical", "Low"))
                   .withColumn("prev_quantity", F.lag("quantity_on_hand", 1).over(by_sku))
                   .withColumn("qty_change_vs_prev", F.col("quantity_on_hand") - F.col("prev_quantity"))
                   .withColumn("is_latest_snapshot", F.col("snapshot_date") == F.max("snapshot_date").over(by_wh)))
    write_table(enriched, silver_path("inventory_enriched"))
    log.info("inventory_enriched written")
except Exception as exc:                          # noqa: BLE001
    log.error("Inventory transformation failed: %s", exc)
    raise

# COMMAND ----------
# Sample execution
out = read_table(silver_path("inventory_enriched"))
print("rows:", out.count(), "| low-stock rows:", out.filter("is_low_stock").count())
out.filter("is_latest_snapshot").groupBy("stock_status").count().orderBy(F.desc("count")).show()
out.filter("is_latest_snapshot AND is_low_stock").select("warehouse_id", "sku", "product_name", "quantity_on_hand",
                                                         "reorder_level", "stock_status", "qty_change_vs_prev").show(8, truncate=False)
