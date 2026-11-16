# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gold analytics
# MAGIC **Goal:** build the 8 analytics-ready tables that answer the 10 business questions. Written as Parquet to `gold/<table>` and copied to Azure SQL by ADF.
# MAGIC
# MAGIC | Gold table | Business question |
# MAGIC |---|---|
# MAGIC | `gold_shipment_performance_summary` | Q1, Q2, Q4, Q6 (monthly) |
# MAGIC | `gold_delivery_delay_analysis` | delay root causes |
# MAGIC | `gold_customer_delay_analysis` | Q9 |
# MAGIC | `gold_warehouse_inventory_summary` | Q5, Q10 |
# MAGIC | `gold_partner_performance` | Q3 |
# MAGIC | `gold_monthly_cost_analysis` | Q8 |
# MAGIC | `gold_route_performance` | Q7 |
# MAGIC | `gold_kpi_summary` | headline answers for Q1-Q10 |
# MAGIC
# MAGIC Metric definition: **on-time % = Delivered / (Delivered + Delayed)**; In-Transit and Cancelled shipments are excluded from the denominator.
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
def status_counts():
    """Conditional-sum columns reused by several Gold tables."""
    s = F.col("shipment_status")
    return [F.sum(F.when(s == "Delivered", 1).otherwise(0)).alias("delivered_on_time"),
            F.sum(F.when(s == "Delayed", 1).otherwise(0)).alias("delayed_shipments"),
            F.sum(F.when(s == "In Transit", 1).otherwise(0)).alias("in_transit_shipments"),
            F.sum(F.when(s == "Cancelled", 1).otherwise(0)).alias("cancelled_shipments")]


def add_rates(df: DataFrame) -> DataFrame:
    completed = F.col("delivered_on_time") + F.col("delayed_shipments")
    return (df.withColumn("delay_rate_pct", pct(F.col("delayed_shipments"), completed))
              .withColumn("on_time_pct", pct(F.col("delivered_on_time"), completed)))


def rank_by(df: DataFrame, *order_cols) -> DataFrame:
    """Dense, unique ranking (1..n) using row_number over the given ordering."""
    return df.withColumn("delay_rank", F.row_number().over(Window.orderBy(*order_cols)))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Gold tables

# COMMAND ----------
se = read_table(silver_path("shipment_enriched")).cache()
ie = read_table(silver_path("inventory_enriched")).cache()
done = F.col("shipment_status").isin("Delivered", "Delayed")

# 1) monthly shipment performance --------------------------------------------------------
perf = (se.groupBy("ship_month").agg(
            F.count("*").alias("total_shipments"),
            F.sum(F.when(F.col("shipment_status") == "Delivered", 1).otherwise(0)).alias("delivered_shipments"),
            F.sum(F.when(F.col("shipment_status") == "Delayed", 1).otherwise(0)).alias("delayed_shipments"),
            F.sum(F.when(F.col("shipment_status") == "In Transit", 1).otherwise(0)).alias("in_transit_shipments"),
            F.sum(F.when(F.col("shipment_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_shipments"),
            F.round(F.avg(F.when(done, F.col("delivery_days"))), 2).alias("avg_delivery_days"))
        .withColumn("on_time_delivery_pct", pct(F.col("delivered_shipments"), F.col("delivered_shipments") + F.col("delayed_shipments")))
        .withColumn("delay_rate_pct", pct(F.col("delayed_shipments"), F.col("delivered_shipments") + F.col("delayed_shipments")))
        .select("ship_month", "total_shipments", "delivered_shipments", "delayed_shipments", "in_transit_shipments",
                "cancelled_shipments", "on_time_delivery_pct", "delay_rate_pct", "avg_delivery_days").orderBy("ship_month"))

# 2) delay root cause x severity -----------------------------------------------------------
delayed = se.filter(F.col("shipment_status") == "Delayed")
total_delayed = delayed.count()
delay = (delayed.groupBy("delay_reason", "delay_bucket").agg(
             F.count("*").alias("delayed_shipments"), F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
             F.sum("delay_days").alias("total_delay_days"), F.round(F.sum("shipping_cost"), 2).alias("total_shipping_cost"))
         .withColumn("pct_of_all_delays", F.round(F.col("delayed_shipments") / F.lit(total_delayed) * 100, 2))
         .orderBy(F.desc("delayed_shipments")))

# 3) customers with most delayed deliveries (Q9) -----------------------------------------------
cust = (se.groupBy("customer_id", "customer_name", "segment").agg(
            F.count("*").alias("total_shipments"),
            F.sum(F.when(done, 1).otherwise(0)).alias("completed_shipments"),
            F.sum(F.when(F.col("shipment_status") == "Delayed", 1).otherwise(0)).alias("delayed_shipments"),
            F.round(F.coalesce(F.avg(F.when(F.col("shipment_status") == "Delayed", F.col("delay_days"))), F.lit(0.0)), 2).alias("avg_delay_days"))
        .withColumn("delay_rate_pct", pct(F.col("delayed_shipments"), F.col("completed_shipments"))))
cust = rank_by(cust, F.desc("delayed_shipments"), F.desc("delay_rate_pct"), F.asc("customer_id"))

# 4) warehouse inventory summary (Q5, Q10) -------------------------------------------------------
latest = ie.filter("is_latest_snapshot")
wh_inv = (latest.groupBy("warehouse_id", "warehouse_name", "city", "region", "capacity_units").agg(
              F.max("snapshot_date").alias("snapshot_date"), F.countDistinct("sku").alias("total_skus"),
              F.sum("quantity_on_hand").alias("total_units"), F.round(F.sum("stock_value"), 2).alias("inventory_value"),
              F.sum(F.when(F.col("is_low_stock"), 1).otherwise(0)).alias("low_stock_skus"),
              F.sum(F.when(F.col("stock_status") == "Critical", 1).otherwise(0)).alias("critical_skus"),
              F.sum(F.when(F.col("stock_status") == "Overstock", 1).otherwise(0)).alias("overstock_skus"))
          .withColumn("low_stock_pct", pct(F.col("low_stock_skus"), F.col("total_skus")))
          .withColumn("capacity_utilization_pct", F.round(F.col("total_units") / F.col("capacity_units") * 100, 2))
          .withColumn("stock_status", F.when(F.col("low_stock_pct") >= 40, "Needs Replenishment")
                                        .when(F.col("low_stock_pct") >= 25, "Watch").otherwise("Healthy"))
          .withColumn("low_stock_rank", F.row_number().over(Window.orderBy(F.desc("low_stock_pct"), F.asc("warehouse_id")))))

# 5) transportation partner performance (Q3) ----------------------------------------------------------
partner = (se.groupBy("partner_id", "partner_name", "transport_mode").agg(
               F.count("*").alias("total_shipments"), *status_counts(),
               F.round(F.avg(F.when(done, F.col("delivery_days"))), 2).alias("avg_delivery_days"),
               F.round(F.avg(F.when(F.col("shipment_status") == "Delayed", F.col("delay_days"))), 2).alias("avg_delay_days"),
               F.round(F.avg(F.when(F.col("sla_breach").isNotNull(), F.col("sla_breach").cast("int"))) * 100, 2).alias("sla_breach_pct"),
               F.round(F.sum("shipping_cost"), 2).alias("total_shipping_cost"),
               F.round(F.avg("shipping_cost"), 2).alias("avg_cost_per_shipment"),
               F.round(F.avg("cost_per_km"), 2).alias("avg_cost_per_km")))
partner = rank_by(add_rates(partner), F.desc("delay_rate_pct"), F.asc("partner_id")).select(
    "partner_id", "partner_name", "transport_mode", "total_shipments", "delivered_on_time", "delayed_shipments",
    "in_transit_shipments", "cancelled_shipments", "on_time_pct", "delay_rate_pct", "avg_delivery_days",
    "avg_delay_days", "sla_breach_pct", "total_shipping_cost", "avg_cost_per_shipment", "avg_cost_per_km", "delay_rank")

# 6) monthly transportation cost (Q8) with window functions --------------------------------------------
monthly = (se.groupBy("ship_month").agg(
               F.count("*").alias("shipments"), F.round(F.sum("shipping_cost"), 2).alias("total_cost"),
               F.round(F.avg("shipping_cost"), 2).alias("avg_cost_per_shipment"),
               F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
               *[F.round(F.sum(F.when(F.col("transport_mode") == m, F.col("shipping_cost")).otherwise(0.0)), 2).alias(f"{m.lower()}_cost")
                 for m in ["Road", "Rail", "Air"]]))
w_all = Window.orderBy("ship_month")
monthly = (monthly.withColumn("cost_per_km", F.round(F.col("total_cost") / F.col("total_distance_km"), 4))
           .withColumn("mom_change_pct", F.round((F.col("total_cost") - F.lag("total_cost").over(w_all)) / F.lag("total_cost").over(w_all) * 100, 2))
           .withColumn("cumulative_cost", F.round(F.sum("total_cost").over(w_all.rowsBetween(Window.unboundedPreceding, Window.currentRow)), 2))
           .select("ship_month", "shipments", "total_cost", "avg_cost_per_shipment", "total_distance_km", "road_cost",
                   "rail_cost", "air_cost", "cost_per_km", "mom_change_pct", "cumulative_cost"))

# 7) route performance (Q7) ---------------------------------------------------------------------------------
route = (se.groupBy("route_id", "origin_warehouse_id", "origin_city", "destination_city").agg(
             F.count("*").alias("total_shipments"), *status_counts(),
             F.round(F.avg(F.when(done, F.col("delivery_days"))), 2).alias("avg_delivery_days"),
             F.round(F.avg(F.when(F.col("shipment_status") == "Delayed", F.col("delay_days"))), 2).alias("avg_delay_days"),
             F.round(F.avg("distance_km"), 2).alias("distance_km"), F.round(F.sum("shipping_cost"), 2).alias("total_shipping_cost")))
route = rank_by(add_rates(route), F.desc("delayed_shipments"), F.desc("delay_rate_pct"), F.asc("route_id")).select(
    "route_id", "origin_warehouse_id", "origin_city", "destination_city", "total_shipments", "delivered_on_time",
    "delayed_shipments", "in_transit_shipments", "cancelled_shipments", "delay_rate_pct", "avg_delivery_days",
    "avg_delay_days", "distance_km", "total_shipping_cost", "delay_rank")

# COMMAND ----------
# MAGIC %md
# MAGIC ### KPI summary (headline answers) and write-out

# COMMAND ----------
try:
    vc = {r["shipment_status"]: r["count"] for r in se.groupBy("shipment_status").count().collect()}
    delivered, delayed_n = vc.get("Delivered", 0), vc.get("Delayed", 0)
    avg_days = se.filter(done).agg(F.avg("delivery_days")).first()[0]
    worst_p, worst_r, top_c = partner.first(), route.first(), cust.first()
    wh_rows = wh_inv.collect()
    kpis = [
        (1, "total_shipments", float(sum(vc.values())), "count", "What is the total number of shipments?"),
        (2, "shipments_delivered_on_time", float(delivered), "count", "Delivered / delayed / in transit / cancelled"),
        (2, "shipments_delayed", float(delayed_n), "count", "Delivered / delayed / in transit / cancelled"),
        (2, "shipments_in_transit", float(vc.get("In Transit", 0)), "count", "Delivered / delayed / in transit / cancelled"),
        (2, "shipments_cancelled", float(vc.get("Cancelled", 0)), "count", "Delivered / delayed / in transit / cancelled"),
        (3, "highest_delay_rate_partner_pct", float(worst_p["delay_rate_pct"]), "percent", f"Highest delay rate: {worst_p['partner_name']} ({worst_p['partner_id']})"),
        (4, "avg_delivery_days", round(float(avg_days), 2), "days", "Average delivery time (ship to delivery)"),
        (5, "low_stock_skus_latest_snapshot", float(sum(r["low_stock_skus"] for r in wh_rows)), "count", "SKU-warehouse positions at/below reorder level"),
        (5, "warehouses_needing_replenishment", float(sum(1 for r in wh_rows if r["stock_status"] == "Needs Replenishment")), "count", "Warehouses with >=40% SKUs low"),
        (6, "on_time_delivery_pct", round(delivered / (delivered + delayed_n) * 100, 2), "percent", "Delivered on time / all completed deliveries"),
        (7, "highest_delay_route_delayed_shipments", float(worst_r["delayed_shipments"]), "count", f"Most delays: route {worst_r['route_id']} ({worst_r['origin_city']} -> {worst_r['destination_city']})"),
        (8, "total_transport_cost", float(monthly.agg(F.sum("total_cost")).first()[0]), "USD", "Sum of monthly transportation cost"),
        (8, "avg_monthly_transport_cost", float(monthly.agg(F.avg("total_cost")).first()[0]), "USD", "Average monthly transportation cost"),
        (9, "highest_delay_customer_delayed_shipments", float(top_c["delayed_shipments"]), "count", f"Most delayed deliveries: {top_c['customer_name']} ({top_c['customer_id']})"),
        (10, "total_units_in_stock_latest", float(sum(r["total_units"] for r in wh_rows)), "units", "Units on hand across all warehouses (latest snapshot)"),
        (10, "total_inventory_value_latest", float(sum(r["inventory_value"] for r in wh_rows)), "USD", "Inventory value across all warehouses (latest snapshot)"),
    ]
    kpi = spark.createDataFrame(kpis, ["business_question_no", "metric", "value", "unit", "description"])

    gold = {"gold_kpi_summary": kpi, "gold_shipment_performance_summary": perf, "gold_delivery_delay_analysis": delay,
            "gold_customer_delay_analysis": cust, "gold_warehouse_inventory_summary": wh_inv,
            "gold_partner_performance": partner, "gold_monthly_cost_analysis": monthly, "gold_route_performance": route}
    for name, frame in gold.items():
        write_table(frame, gold_path(name))
        log.info("gold %-36s written", name)
except Exception as exc:                          # noqa: BLE001
    log.error("Gold build failed: %s", exc)
    raise

# COMMAND ----------
# Sample execution
read_table(gold_path("gold_kpi_summary")).orderBy("business_question_no", "metric").show(20, truncate=False)
read_table(gold_path("gold_partner_performance")).select("partner_name", "delayed_shipments", "delay_rate_pct", "avg_delivery_days", "delay_rank").show(truncate=False)
read_table(gold_path("gold_warehouse_inventory_summary")).select("warehouse_id", "city", "total_units", "low_stock_skus", "low_stock_pct", "stock_status").show(truncate=False)
