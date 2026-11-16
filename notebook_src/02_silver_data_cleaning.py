# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver data cleaning
# MAGIC **Goal:** turn Bronze (raw strings) into clean, typed, validated tables. Nothing is silently dropped: bad rows go to `rejected_records` with **one** reason (the first rule that fires), so `bronze = silver + rejected`.
# MAGIC
# MAGIC Steps per table: trim / blank -> NULL, standardise dates, normalise vocabularies, de-duplicate (keep first), validate primary & foreign keys, apply business rules.
# MAGIC
# MAGIC **Business rules**
# MAGIC * `shipment_status` is **derived from the dates** (delivered on/before promised = *Delivered*, later = *Delayed*); the source label is not trusted.
# MAGIC * Missing/invalid `shipping_cost` is imputed as `distance_km x contract_rate x priority_factor` and flagged `cost_imputed`.
# MAGIC * Delivery date earlier than ship date, missing warehouse/customer, orphan keys -> quarantined.
# MAGIC
# MAGIC > **Status:** not executed in this delivery (PySpark unavailable in the build environment) - see the "Expected output" cell for reference values from the pandas implementation.

# {{COMMON}}

# COMMAND ----------
# MAGIC %md
# MAGIC ### Reusable helpers

# COMMAND ----------
STATUS_ALIASES = {"delivered": "Delivered", "delayed": "Delayed", "late": "Delayed", "in transit": "In Transit",
                  "in-transit": "In Transit", "intransit": "In Transit", "cancelled": "Cancelled", "canceled": "Cancelled"}
SEGMENTS = {"retail": "Retail", "wholesale": "Wholesale", "e-commerce": "E-commerce",
            "manufacturing": "Manufacturing", "healthcare": "Healthcare"}
VALID_EVENT_TYPES = ["ORDER_PICKED_UP", "ARRIVED_AT_HUB", "DEPARTED_HUB", "OUT_FOR_DELIVERY", "DELIVERED",
                     "DELAY_EXCEPTION", "CANCELLED"]
PRIORITY_FACTOR = {"Economy": 0.9, "Standard": 1.0, "Express": 1.3}
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"


def map_col(mapping: dict, key_col):
    """Spark map literal lookup: map_col({'a': 1}, F.col('x'))."""
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])[key_col]


def raw_cols(df: DataFrame):
    return [c for c in df.columns if not c.startswith("_")]


def trim_blank_to_null(df: DataFrame) -> DataFrame:
    for c in raw_cols(df):
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))))
    return df


def parse_date(col):
    """Mixed formats -> DATE. ISO yyyy-MM-dd | dd/MM/yyyy | dd-MMM-yyyy. Anything else (or an impossible date) -> NULL."""
    c = F.trim(F.col(col)) if isinstance(col, str) else F.trim(col)
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(c, "yyyy-MM-dd"))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(c, "dd/MM/yyyy"))
             .when(c.rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"), F.to_date(c, "dd-MMM-yyyy")))


def parse_ts(col):
    """Mixed timestamp formats -> TIMESTAMP (falls back to date-only formats at midnight)."""
    c = F.trim(F.col(col)) if isinstance(col, str) else F.trim(col)
    return (F.when(c.rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))
             .when(c.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"), F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"))
             .when(c.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$"), F.to_timestamp(c, "dd/MM/yyyy HH:mm"))
             .when(c.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_timestamp(c, "yyyy-MM-dd")))


def with_dup_rank(df: DataFrame, keys, name: str = "_rn") -> DataFrame:
    """row_number() per key in file order; rank > 1 means 'duplicate of an earlier row'."""
    return df.withColumn(name, F.row_number().over(Window.partitionBy(*keys).orderBy("_row_id")))


def with_fk_flag(df: DataFrame, fk_col: str, parent: DataFrame, parent_key: str, flag: str) -> DataFrame:
    """Adds a boolean column that is TRUE when fk_col has a value that does NOT exist in the parent table."""
    keys = parent.select(F.col(parent_key).alias("_pk")).distinct().withColumn("_exists", F.lit(True))
    return (df.join(F.broadcast(keys), df[fk_col] == keys["_pk"], "left")
              .withColumn(flag, F.col("_exists").isNull() & F.col(fk_col).isNotNull()).drop("_pk", "_exists"))


def split_clean_rejected(df: DataFrame, table: str, key: str, rules, original_cols):
    """Apply ordered (reason, condition) rules. First matching rule wins. Returns (clean, rejected)."""
    reason = F.coalesce(*[F.when(cond, F.lit(r)) for r, cond in rules])
    tagged = df.withColumn("reject_reason", reason)
    rejected = (tagged.filter(F.col("reject_reason").isNotNull())
                .select(F.lit(table).alias("source_table"),
                        F.coalesce(F.col(key).cast("string"), F.lit("<NULL>")).alias("record_key"),
                        "reject_reason", F.to_json(F.struct(*original_cols)).alias("raw_record")))
    clean = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    return clean, rejected


def finalize(df: DataFrame) -> DataFrame:
    """Keep business columns + bronze_batch_id for lineage; drop the other technical columns."""
    df = df.withColumnRenamed("_batch_id", "bronze_batch_id")
    return df.select([c for c in df.columns if not c.startswith("_") and not c.startswith("tmp_")])

# COMMAND ----------
# MAGIC %md
# MAGIC ### Dimension tables

# COMMAND ----------
def clean_customers(bronze: DataFrame):
    cols = raw_cols(bronze)
    df = with_dup_rank(trim_blank_to_null(bronze), ["customer_id"])
    rules = [("MISSING_CUSTOMER_ID", F.col("customer_id").isNull()), ("DUPLICATE_CUSTOMER_ID", F.col("_rn") > 1)]
    clean, rej = split_clean_rejected(df, "customers", "customer_id", rules, cols)
    clean = (clean.withColumn("registration_date", parse_date("registration_date"))
             .withColumn("segment", F.coalesce(map_col(SEGMENTS, F.lower(F.col("segment"))), F.lit("Other")))
             .withColumn("email_status", F.when(F.col("email").isNull(), "Missing")
                         .when(F.col("email").rlike(EMAIL_RE), "Valid").otherwise("Invalid"))
             .withColumn("email", F.when(F.col("email_status") == "Valid", F.col("email"))))   # never keep an invalid address
    return finalize(clean), rej


def clean_warehouses(bronze: DataFrame):
    cols = raw_cols(bronze)
    df = with_dup_rank(trim_blank_to_null(bronze), ["warehouse_id"]).withColumn("tmp_cap", F.col("capacity_units").cast("int"))
    rules = [("MISSING_WAREHOUSE_ID", F.col("warehouse_id").isNull()), ("DUPLICATE_WAREHOUSE_ID", F.col("_rn") > 1),
             ("INVALID_CAPACITY", F.col("tmp_cap").isNull() | (F.col("tmp_cap") <= 0))]
    clean, rej = split_clean_rejected(df, "warehouses", "warehouse_id", rules, cols)
    clean = (clean.withColumn("capacity_units", F.col("tmp_cap")).withColumn("opened_date", parse_date("opened_date"))
             .withColumn("manager_name", F.coalesce(F.col("manager_name"), F.lit("Unassigned"))))
    return finalize(clean), rej


def clean_partners(bronze: DataFrame):
    cols = raw_cols(bronze)
    df = (with_dup_rank(trim_blank_to_null(bronze), ["partner_id"])
          .withColumn("transport_mode", F.initcap(F.col("transport_mode")))
          .withColumn("tmp_rate", F.col("contract_rate_per_km").cast("double")))
    rules = [("MISSING_PARTNER_ID", F.col("partner_id").isNull()), ("DUPLICATE_PARTNER_ID", F.col("_rn") > 1),
             ("INVALID_TRANSPORT_MODE", ~F.col("transport_mode").isin("Road", "Rail", "Air")),
             ("INVALID_CONTRACT_RATE", F.col("tmp_rate").isNull() | (F.col("tmp_rate") <= 0))]
    clean, rej = split_clean_rejected(df, "transportation_partners", "partner_id", rules, cols)
    clean = (clean.withColumn("contract_rate_per_km", F.col("tmp_rate")).withColumn("sla_days", F.col("sla_days").cast("int"))
             .withColumn("contract_start_date", parse_date("contract_start_date"))
             .withColumn("contact_email", F.coalesce(F.col("contact_email"), F.lit("unknown")))
             .withColumn("is_active", F.upper(F.col("is_active")) == "Y"))
    return finalize(clean), rej

# COMMAND ----------
# MAGIC %md
# MAGIC ### Shipments (PK / FK validation, derived status, cost imputation)

# COMMAND ----------
def clean_shipments(bronze: DataFrame, customers: DataFrame, warehouses: DataFrame, partners: DataFrame):
    cols = raw_cols(bronze)
    df = with_dup_rank(trim_blank_to_null(bronze), ["shipment_id"])
    df = (df.withColumn("tmp_ship", parse_date("ship_date"))
            .withColumn("tmp_prom", parse_date("promised_delivery_date"))
            .withColumn("tmp_act", parse_date("actual_delivery_date"))
            .withColumn("tmp_status", map_col(STATUS_ALIASES, F.lower(F.col("shipment_status")))))
    df = with_fk_flag(df, "origin_warehouse_id", warehouses, "warehouse_id", "tmp_orphan_wh")
    df = with_fk_flag(df, "customer_id", customers, "customer_id", "tmp_orphan_cust")
    df = with_fk_flag(df, "partner_id", partners, "partner_id", "tmp_orphan_partner")
    rules = [
        ("MISSING_SHIPMENT_ID", F.col("shipment_id").isNull()),
        ("DUPLICATE_SHIPMENT_ID", F.col("_rn") > 1),
        ("INVALID_SHIPMENT_STATUS", F.col("tmp_status").isNull()),
        ("INVALID_SHIP_OR_PROMISED_DATE", F.col("tmp_ship").isNull() | F.col("tmp_prom").isNull()),
        ("INVALID_DELIVERY_DATE", F.col("actual_delivery_date").isNotNull() & F.col("tmp_act").isNull()),
        ("MISSING_WAREHOUSE_ID", F.col("origin_warehouse_id").isNull()),
        ("ORPHAN_WAREHOUSE_ID", F.col("tmp_orphan_wh")),
        ("MISSING_CUSTOMER_ID", F.col("customer_id").isNull()),
        ("ORPHAN_CUSTOMER_ID", F.col("tmp_orphan_cust")),
        ("ORPHAN_PARTNER_ID", F.col("tmp_orphan_partner") | F.col("partner_id").isNull()),
        ("DELIVERY_BEFORE_SHIPMENT", F.col("tmp_act") < F.col("tmp_ship")),
        ("PROMISED_BEFORE_SHIPMENT", F.col("tmp_prom") < F.col("tmp_ship")),
        ("MISSING_DELIVERY_DATE_FOR_DELIVERED", F.col("tmp_status").isin("Delivered", "Delayed") & F.col("tmp_act").isNull()),
    ]
    clean, rej = split_clean_rejected(df, "shipments", "shipment_id", rules, cols)

    # numeric columns (non-numeric text such as 'N/A' or '12kg' becomes NULL)
    clean = (clean.withColumn("weight_kg", F.col("weight_kg").cast("double"))
                  .withColumn("distance_km", F.col("distance_km").cast("double"))
                  .withColumn("shipping_cost", F.col("shipping_cost").cast("double")))
    median_weight = clean.approxQuantile("weight_kg", [0.5], 0.001)[0]
    rates = partners.select("partner_id", F.col("contract_rate_per_km").cast("double").alias("tmp_rate"))
    clean = clean.join(F.broadcast(rates), "partner_id", "left")
    estimate = F.round(F.col("distance_km") * F.col("tmp_rate") * F.coalesce(map_col(PRIORITY_FACTOR, F.col("priority")), F.lit(1.0)), 2)
    bad_cost = F.col("shipping_cost").isNull() | (F.col("shipping_cost") <= 0)

    clean = (clean
             .withColumn("weight_imputed", F.col("weight_kg").isNull())
             .withColumn("weight_kg", F.round(F.coalesce(F.col("weight_kg"), F.lit(median_weight)), 1))
             .withColumn("cost_imputed", bad_cost)
             .withColumn("shipping_cost", F.when(bad_cost, estimate).otherwise(F.col("shipping_cost")))
             .withColumn("ship_date", F.col("tmp_ship")).withColumn("promised_delivery_date", F.col("tmp_prom")))

    # business rule: derive status from the dates
    delivered = F.col("tmp_act").isNotNull() & (F.col("tmp_status") != "Cancelled")
    status = (F.when(delivered, F.when(F.col("tmp_act") <= F.col("tmp_prom"), "Delivered").otherwise("Delayed"))
               .otherwise(F.col("tmp_status")))
    clean = (clean.withColumn("shipment_status", status)
             .withColumn("status_corrected", F.col("shipment_status") != F.col("tmp_status"))
             .withColumn("actual_delivery_date", F.when(F.col("shipment_status") == "Cancelled", F.lit(None).cast("date")).otherwise(F.col("tmp_act")))
             .withColumn("planned_transit_days", F.datediff("promised_delivery_date", "ship_date"))
             .withColumn("delivery_days", F.datediff("actual_delivery_date", "ship_date"))
             .withColumn("delay_days", F.greatest(F.datediff("actual_delivery_date", "promised_delivery_date"), F.lit(0))
                         .cast("int"))
             .withColumn("delay_days", F.when(F.col("actual_delivery_date").isNull(), F.lit(None)).otherwise(F.col("delay_days")))
             .withColumn("is_on_time", F.when(F.col("shipment_status").isin("Delivered", "Delayed"),
                                              (F.col("shipment_status") == "Delivered").cast("int")))
             .withColumn("ship_month", F.date_format("ship_date", "yyyy-MM"))
             .withColumn("cost_per_km", F.round(F.col("shipping_cost") / F.col("distance_km"), 4)))
    return finalize(clean), rej

# COMMAND ----------
# MAGIC %md
# MAGIC ### Tracking events and inventory

# COMMAND ----------
def clean_tracking_events(bronze: DataFrame, shipments_clean: DataFrame):
    cols = raw_cols(bronze)
    df = with_dup_rank(trim_blank_to_null(bronze), ["event_id"])
    df = (df.withColumn("event_type", F.upper(F.col("event_type"))).withColumn("tmp_ts", parse_ts("event_timestamp")))
    df = with_fk_flag(df, "shipment_id", shipments_clean, "shipment_id", "tmp_orphan_ship")
    rules = [("MISSING_EVENT_ID", F.col("event_id").isNull()), ("DUPLICATE_EVENT_ID", F.col("_rn") > 1),
             ("INVALID_EVENT_TYPE", ~F.col("event_type").isin(*VALID_EVENT_TYPES)),
             ("MISSING_EVENT_TIMESTAMP", F.col("event_timestamp").isNull()),
             ("INVALID_EVENT_TIMESTAMP", F.col("tmp_ts").isNull()),
             ("ORPHAN_SHIPMENT_ID", F.col("tmp_orphan_ship") | F.col("shipment_id").isNull())]
    stage1, rej1 = split_clean_rejected(df, "shipment_tracking_events", "event_id", rules, cols)
    # second pass: identical (shipment, type, timestamp) among surviving rows is a content duplicate
    stage1 = stage1.withColumn("_rn2", F.row_number().over(Window.partitionBy("shipment_id", "event_type", "tmp_ts").orderBy("_row_id")))
    clean, rej2 = split_clean_rejected(stage1, "shipment_tracking_events", "event_id",
                                       [("DUPLICATE_EVENT_CONTENT", F.col("_rn2") > 1)], cols)
    clean = clean.withColumn("event_timestamp", F.col("tmp_ts"))
    return finalize(clean), rej1.unionByName(rej2)


def clean_inventory(bronze: DataFrame, warehouses: DataFrame):
    cols = raw_cols(bronze)
    df = with_dup_rank(trim_blank_to_null(bronze), ["inventory_id"])
    df = (df.withColumn("tmp_snap", parse_date("snapshot_date")).withColumn("tmp_qty", F.col("quantity_on_hand").cast("int")))
    df = with_fk_flag(df, "warehouse_id", warehouses, "warehouse_id", "tmp_orphan_wh")
    rules = [("MISSING_INVENTORY_ID", F.col("inventory_id").isNull()), ("DUPLICATE_INVENTORY_ID", F.col("_rn") > 1),
             ("MISSING_WAREHOUSE_ID", F.col("warehouse_id").isNull()), ("ORPHAN_WAREHOUSE_ID", F.col("tmp_orphan_wh")),
             ("INVALID_SNAPSHOT_DATE", F.col("tmp_snap").isNull()), ("MISSING_QUANTITY", F.col("tmp_qty").isNull()),
             ("NEGATIVE_QUANTITY", F.col("tmp_qty") < 0)]
    stage1, rej1 = split_clean_rejected(df, "inventory", "inventory_id", rules, cols)
    stage1 = stage1.withColumn("_rn2", F.row_number().over(Window.partitionBy("warehouse_id", "sku", "tmp_snap").orderBy("_row_id")))
    clean, rej2 = split_clean_rejected(stage1, "inventory", "inventory_id",
                                       [("DUPLICATE_WAREHOUSE_SKU_SNAPSHOT", F.col("_rn2") > 1)], cols)
    clean = (clean.withColumn("snapshot_date", F.col("tmp_snap")).withColumn("quantity_on_hand", F.col("tmp_qty"))
             .withColumn("reorder_level", F.col("reorder_level").cast("int")).withColumn("unit_cost", F.col("unit_cost").cast("double")))
    return finalize(clean), rej1.unionByName(rej2)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Run: clean in dependency order, write Silver + quarantine

# COMMAND ----------
try:
    b = {t: read_table(bronze_path(t)) for t in ["customers", "warehouses", "transportation_partners", "shipments",
                                                  "shipment_tracking_events", "inventory"]}
    customers, r1 = clean_customers(b["customers"])
    warehouses, r2 = clean_warehouses(b["warehouses"])
    partners, r3 = clean_partners(b["transportation_partners"])
    shipments, r4 = clean_shipments(b["shipments"], customers, warehouses, partners)
    events, r5 = clean_tracking_events(b["shipment_tracking_events"], shipments)
    inventory, r6 = clean_inventory(b["inventory"], warehouses)
    rejected = r1.unionByName(r2).unionByName(r3).unionByName(r4).unionByName(r5).unionByName(r6)

    silver = {"customers": customers, "warehouses": warehouses, "transportation_partners": partners,
              "shipments": shipments, "shipment_tracking_events": events, "inventory": inventory,
              "rejected_records": rejected}
    for name, frame in silver.items():
        write_table(frame, silver_path(name))
        log.info("silver %-26s written", name)
except Exception as exc:                          # noqa: BLE001
    log.error("Silver cleaning failed: %s", exc)
    raise

# COMMAND ----------
# Sample execution: reconciliation  bronze = silver + rejected  (must be 0 difference everywhere)
rej_counts = {r["source_table"]: r["n"] for r in read_table(silver_path("rejected_records")).groupBy("source_table").agg(F.count("*").alias("n")).collect()}
rows = []
for t in ["customers", "warehouses", "transportation_partners", "shipments", "shipment_tracking_events", "inventory"]:
    nb, ns, nr = b[t].count(), read_table(silver_path(t)).count(), rej_counts.get(t, 0)
    rows.append((t, nb, ns, nr, nb - ns - nr))
spark.createDataFrame(rows, ["table", "bronze_rows", "silver_rows", "rejected_rows", "difference"]).show(truncate=False)

# COMMAND ----------
read_table(silver_path("rejected_records")).groupBy("source_table", "reject_reason").count().orderBy("source_table", F.desc("count")).show(60, truncate=False)
