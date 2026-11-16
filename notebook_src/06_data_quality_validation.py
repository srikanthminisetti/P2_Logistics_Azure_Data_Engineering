# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Data-quality validation
# MAGIC **Goal:** automated checks on Bronze (to *measure* source quality) and Silver (must be clean). The notebook **fails the ADF pipeline** if any Silver ERROR-level check fails.
# MAGIC
# MAGIC Checks: null values, duplicate keys, invalid dates, referential integrity, allowed values (status / event type), negative quantities, delivery before shipment, unexpected data types, and Bronze = Silver + Rejected reconciliation.
# MAGIC
# MAGIC Output: `gold/data_quality_report` (one row per check).
# MAGIC
# MAGIC > **Status:** not executed in this delivery (PySpark unavailable in the build environment) - see "Expected output".

# {{COMMON}}

# COMMAND ----------
import re as _re   # noqa: E402

STATUS_ALIASES = {"delivered": "Delivered", "delayed": "Delayed", "late": "Delayed", "in transit": "In Transit",
                  "in-transit": "In Transit", "intransit": "In Transit", "cancelled": "Cancelled", "canceled": "Cancelled"}
VALID_STATUSES = ["Delivered", "Delayed", "In Transit", "Cancelled"]
VALID_EVENT_TYPES = ["ORDER_PICKED_UP", "ARRIVED_AT_HUB", "DEPARTED_HUB", "OUT_FOR_DELIVERY", "DELIVERED", "DELAY_EXCEPTION", "CANCELLED"]
DATE_PATTERNS = [r"^\d{4}-\d{2}-\d{2}$", r"^\d{2}/\d{2}/\d{4}$", r"^\d{2}-[A-Za-z]{3}-\d{4}$"]
results = []   # list of dict rows


def record(layer, table, check, column, df_total, failed, severity="ERROR"):
    status = "PASS" if failed == 0 else ("FAIL" if severity == "ERROR" else "WARN")
    results.append({"layer": layer, "table": table, "check_name": check, "column": column, "rows_checked": int(df_total),
                    "rows_failed": int(failed), "failure_pct": round(failed / max(df_total, 1) * 100, 3), "severity": severity, "status": status})


def blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")


def check_not_null(df, layer, table, col, severity="ERROR"):
    record(layer, table, "not_null", col, df.count(), df.filter(blank(col)).count(), severity)


def check_unique(df, layer, table, col):
    n = df.filter(F.col(col).isNotNull()).count()
    record(layer, table, "unique_key", col, df.count(), n - df.select(col).filter(F.col(col).isNotNull()).distinct().count())


def check_fk(df, layer, table, col, parent, parent_col, parent_name):
    keys = parent.select(F.col(parent_col).alias("_k")).distinct()
    bad = df.filter(~blank(col)).join(F.broadcast(keys), df[col] == keys["_k"], "left_anti").count()
    record(layer, table, f"referential_integrity->{parent_name}", col, df.count(), bad)


def check_allowed(df, layer, table, col, allowed, normaliser=lambda c: c):
    record(layer, table, "allowed_values", col, df.count(), df.filter(~blank(col) & ~normaliser(F.col(col)).isin(*allowed)).count())


def check_dates(df, layer, table, col):
    """Bronze: present but matches no supported pattern or is an impossible date. Silver: column is already DATE (0 failures)."""
    if dict(df.dtypes)[col] in ("date", "timestamp"):
        record(layer, table, "valid_dates", col, df.count(), 0)
        return
    ok = F.lit(False)
    for pat, fmt in zip(DATE_PATTERNS, ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy"]):
        ok = ok | (F.col(col).rlike(pat) & F.to_date(F.col(col), fmt).isNotNull())
    record(layer, table, "valid_dates", col, df.count(), df.filter(~blank(col) & ~ok).count())


def check_numeric(df, layer, table, col):
    record(layer, table, "data_type_numeric", col, df.count(), df.filter(~blank(col) & F.col(col).cast("double").isNull()).count())


def check_non_negative(df, layer, table, col):
    record(layer, table, "non_negative", col, df.count(), df.filter(F.col(col).cast("double") < 0).count())


def check_delivery_order(df, layer, table):
    """Delivery earlier than ship date. Works on Bronze strings (mixed formats) and Silver dates."""
    def as_date(c):
        if dict(df.dtypes)[c] == "date":
            return F.col(c)
        return (F.when(F.col(c).rlike(DATE_PATTERNS[0]), F.to_date(F.col(c), "yyyy-MM-dd"))
                 .when(F.col(c).rlike(DATE_PATTERNS[1]), F.to_date(F.col(c), "dd/MM/yyyy"))
                 .when(F.col(c).rlike(DATE_PATTERNS[2]), F.to_date(F.col(c), "dd-MMM-yyyy")))
    record(layer, table, "delivery_date_order", "actual_delivery_date", df.count(),
           df.filter(as_date("actual_delivery_date") < as_date("ship_date")).count())

# COMMAND ----------
def run_suite(layer: str, t: dict):
    cust, wh, par, ship, ev, inv = (t[k] for k in ["customers", "warehouses", "transportation_partners", "shipments",
                                                    "shipment_tracking_events", "inventory"])
    for name, df, key in [("customers", cust, "customer_id"), ("warehouses", wh, "warehouse_id"), ("transportation_partners", par, "partner_id"),
                          ("shipments", ship, "shipment_id"), ("shipment_tracking_events", ev, "event_id"), ("inventory", inv, "inventory_id")]:
        check_not_null(df, layer, name, key)
        check_unique(df, layer, name, key)
    for c in ["customer_id", "origin_warehouse_id", "partner_id", "ship_date", "promised_delivery_date", "shipment_status"]:
        check_not_null(ship, layer, "shipments", c)
    check_not_null(ship, layer, "shipments", "shipping_cost", "WARN")
    check_not_null(ship, layer, "shipments", "actual_delivery_date", "WARN")     # NULL is legal for In Transit / Cancelled
    for c in ["ship_date", "promised_delivery_date", "actual_delivery_date"]:
        check_dates(ship, layer, "shipments", c)
    if layer == "bronze":
        bad = ship.filter(~blank("shipment_status") & ~F.lower(F.trim(F.col("shipment_status"))).isin(*STATUS_ALIASES.keys())).count()
        record(layer, "shipments", "allowed_values", "shipment_status", ship.count(), bad)
    else:
        check_allowed(ship, layer, "shipments", "shipment_status", VALID_STATUSES)
    check_fk(ship, layer, "shipments", "customer_id", cust, "customer_id", "customers")
    check_fk(ship, layer, "shipments", "origin_warehouse_id", wh, "warehouse_id", "warehouses")
    check_fk(ship, layer, "shipments", "partner_id", par, "partner_id", "transportation_partners")
    check_delivery_order(ship, layer, "shipments")
    for c in ["weight_kg", "distance_km", "shipping_cost"]:
        check_numeric(ship, layer, "shipments", c)
    check_non_negative(ship, layer, "shipments", "shipping_cost")
    check_not_null(ev, layer, "shipment_tracking_events", "event_timestamp")
    check_allowed(ev, layer, "shipment_tracking_events", "event_type", VALID_EVENT_TYPES, F.upper)
    check_fk(ev, layer, "shipment_tracking_events", "shipment_id", ship, "shipment_id", "shipments")
    check_not_null(inv, layer, "inventory", "warehouse_id")
    check_not_null(inv, layer, "inventory", "quantity_on_hand")
    check_non_negative(inv, layer, "inventory", "quantity_on_hand")
    check_dates(inv, layer, "inventory", "snapshot_date")
    check_fk(inv, layer, "inventory", "warehouse_id", wh, "warehouse_id", "warehouses")
    check_numeric(inv, layer, "inventory", "quantity_on_hand")
    check_not_null(cust, layer, "customers", "email", "WARN")

# COMMAND ----------
try:
    tables = ["customers", "warehouses", "transportation_partners", "shipments", "shipment_tracking_events", "inventory"]
    bronze = {t: read_table(bronze_path(t)) for t in tables}
    silver = {t: read_table(silver_path(t)) for t in tables}
    rejected = read_table(silver_path("rejected_records"))

    run_suite("bronze", bronze)      # measures source quality - failures are EXPECTED here
    run_suite("silver", silver)      # must be clean
    rej = {r["source_table"]: r["n"] for r in rejected.groupBy("source_table").agg(F.count("*").alias("n")).collect()}
    for t in tables:                 # reconciliation: nothing lost between layers
        nb, ns, nr = bronze[t].count(), silver[t].count(), rej.get(t, 0)
        record("reconciliation", t, "bronze=silver+rejected", "*", nb, abs(nb - ns - nr))

    report = spark.createDataFrame(results)
    write_table(report, gold_path("data_quality_report"))
except Exception as exc:                          # noqa: BLE001
    log.error("Data-quality run crashed: %s", exc)
    raise

# COMMAND ----------
# Sample execution: summary by layer/status, then the worst source issues
report.groupBy("layer", "status").count().orderBy("layer", "status").show()
report.filter("layer = 'bronze' AND rows_failed > 0").orderBy(F.desc("rows_failed")).select("table", "check_name", "column", "rows_failed", "severity").show(15, truncate=False)

# COMMAND ----------
# Gate: fail the notebook (and therefore the ADF pipeline) when Silver has ERROR-level failures
silver_failures = report.filter("layer IN ('silver', 'reconciliation') AND status = 'FAIL'")
n_fail = silver_failures.count()
if n_fail > 0:
    silver_failures.show(truncate=False)
    raise AssertionError(f"{n_fail} Silver / reconciliation data-quality checks failed")
log.info("All Silver data-quality checks passed")
