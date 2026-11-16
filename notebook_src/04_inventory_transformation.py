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

# {{COMMON}}

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
