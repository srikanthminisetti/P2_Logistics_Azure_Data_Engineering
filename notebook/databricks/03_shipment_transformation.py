# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Shipment transformation (Silver -> enriched shipment fact)
# MAGIC **Goal:** one analytics-friendly row per shipment, enriched with customer / warehouse / partner attributes and a summary of its tracking events.
# MAGIC
# MAGIC Techniques: broadcast **joins**, **groupBy/agg** on events, **window function** (`row_number`) to pick the latest event, `when/otherwise` business rules.
# MAGIC
# MAGIC New columns: `events_count`, `first_event_ts`, `last_event_ts`, `last_event_type`, `delay_reason`, `delay_bucket`, `sla_breach`.
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
def summarise_events(events: DataFrame) -> DataFrame:
    """One row per shipment: counts, first/last timestamps, last event type (window) and delay reason."""
    agg = events.groupBy("shipment_id").agg(
        F.count("*").alias("events_count"),
        F.min("event_timestamp").alias("first_event_ts"),
        F.max("event_timestamp").alias("last_event_ts"))
    latest = (events.withColumn("rn", F.row_number().over(Window.partitionBy("shipment_id").orderBy(F.col("event_timestamp").desc())))
                    .filter("rn = 1").select("shipment_id", F.col("event_type").alias("last_event_type")))
    reasons = (events.filter(F.col("event_type") == "DELAY_EXCEPTION")
                     .withColumn("rn", F.row_number().over(Window.partitionBy("shipment_id").orderBy("event_timestamp")))
                     .filter("rn = 1").select("shipment_id", F.col("remarks").alias("delay_reason")))
    return agg.join(latest, "shipment_id", "left").join(reasons, "shipment_id", "left")


def delay_bucket(days_col):
    return (F.when(days_col.isNull() | (days_col <= 0), "On time").when(days_col == 1, "1 day")
             .when(days_col <= 3, "2-3 days").otherwise("4+ days"))

# COMMAND ----------
try:
    ship = read_table(silver_path("shipments"))
    cust = read_table(silver_path("customers")).select("customer_id", "customer_name", "segment")
    wh = read_table(silver_path("warehouses")).select("warehouse_id", "warehouse_name",
                                                      F.col("city").alias("origin_city"), F.col("region").alias("origin_region"))
    par = read_table(silver_path("transportation_partners")).select("partner_id", "partner_name", "transport_mode", "sla_days")
    events = summarise_events(read_table(silver_path("shipment_tracking_events")))

    enriched = (ship.join(F.broadcast(cust), "customer_id", "left")
                    .join(F.broadcast(wh), ship["origin_warehouse_id"] == wh["warehouse_id"], "left").drop("warehouse_id")
                    .join(F.broadcast(par), "partner_id", "left")
                    .join(events, "shipment_id", "left")
                    .withColumn("delay_bucket", delay_bucket(F.col("delay_days")))
                    .withColumn("delay_reason", F.when(F.col("shipment_status") == "Delayed", F.coalesce(F.col("delay_reason"), F.lit("Unspecified"))))
                    .withColumn("sla_breach", F.when(F.col("delivery_days").isNotNull(), F.col("delivery_days") > F.col("sla_days")))
                    .withColumn("events_count", F.coalesce(F.col("events_count"), F.lit(0))))
    write_table(enriched, silver_path("shipment_enriched"))
    log.info("shipment_enriched written")
except Exception as exc:                          # noqa: BLE001
    log.error("Shipment transformation failed: %s", exc)
    raise

# COMMAND ----------
# Sample execution
out = read_table(silver_path("shipment_enriched"))
print("rows:", out.count())
out.groupBy("shipment_status").count().orderBy(F.desc("count")).show()
out.filter("shipment_status = 'Delayed'").groupBy("delay_reason").count().orderBy(F.desc("count")).show()
out.select("shipment_id", "customer_name", "partner_name", "origin_city", "destination_city", "shipment_status",
           "delivery_days", "delay_days", "delay_bucket", "last_event_type", "sla_breach").show(5, truncate=False)
