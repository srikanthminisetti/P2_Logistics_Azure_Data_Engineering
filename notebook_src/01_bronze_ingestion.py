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

# {{COMMON}}

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
