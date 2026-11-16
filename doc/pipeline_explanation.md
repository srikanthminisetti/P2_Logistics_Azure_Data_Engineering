# Pipeline Explanation

![ADF flow](../architecture/adf_orchestration_flow.png)

## ADF object inventory
| Type | Name | Purpose |
|---|---|---|
| Linked service | `LS_KeyVault` | Key Vault connection (managed identity) |
| | `LS_ADLS_Gen2` | ADLS Gen2, **parameterised by `storage_account`**, managed identity |
| | `LS_AzureSqlDatabase` | Azure SQL; password = Key Vault secret `sql-admin-password` |
| | `LS_AzureDatabricks` | job-cluster notebooks; token = Key Vault secret `databricks-access-token` |
| Dataset | `DS_ADLS_Binary` | byte-for-byte file copy (params: storage_account, container, folder, file_name) |
| | `DS_ADLS_DelimitedText` | CSV with header (reusable) |
| | `DS_ADLS_Json` | the metadata config file |
| | `DS_ADLS_Parquet` | Gold / Silver Parquet folders |
| | `DS_AzureSql_Table` | parameterised SQL table (schema_name, table_name) |
| Pipeline | `PL_Master_Logistics` | orchestration, logging, alerting |
| | `PL_Ingest_Raw_To_Bronze` | Lookup + ForEach + Copy |
| | `PL_Run_Databricks_Transformations` | notebooks 01-06 |
| | `PL_Load_Gold_To_SQL` | SP reset + 3 ForEach loads |
| Trigger | `tr_daily_logistics` | schedule, 02:00 UTC |

## Activity flow
```
PL_Master_Logistics
 SP_Log_Pipeline_Start
   -> Execute_Ingest        (PL_Ingest_Raw_To_Bronze)
   -> Execute_Transformations (PL_Run_Databricks_Transformations)
   -> Execute_Load_Gold      (PL_Load_Gold_To_SQL)
   -> SP_Log_Pipeline_Success
 on Failed of each Execute_*: Notify_Failure_<stage> (Web) -> SP_Log_Failure_<stage> -> Fail_Pipeline_<stage>

PL_Ingest_Raw_To_Bronze:  Lookup_Source_Config -> ForEach_Source_Table[ Copy_Landing_To_Bronze ]
PL_Run_Databricks_Transformations:
   NB_01 -> NB_02 -> (NB_03 || NB_04) -> NB_05 -> NB_06
PL_Load_Gold_To_SQL:      SP_Prepare_Reload -> ForEach_Load_Dimension -> ForEach_Load_Fact -> ForEach_Load_Gold
```
Why separate failure handlers per stage? In ADF an activity runs only when **all** its `dependsOn` conditions are met, so one handler cannot depend on "Failed" of three different activities. One handler per stage is the standard pattern.

## Parameterised expressions explained
| Expression | Where | Meaning |
|---|---|---|
| `@pipeline().parameters.run_date` | all pipelines | the logical date being processed (the trigger passes it) |
| `@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')` | trigger | converts the scheduled time into the `run_date` string |
| `@activity('Lookup_Source_Config').output.value` | ForEach `items` | array returned by the Lookup (one object per source table) |
| `@item().table_name`, `@item().file_name`, `@item().source_system` | inside ForEach | current element of the loop; **dynamic file paths** |
| `@concat('raw_files/', item().table_name, '/ingest_date=', formatDateTime(pipeline().parameters.run_date, 'yyyy-MM-dd'))` | Copy sink folder | builds `raw_files/shipments/ingest_date=2026-06-30` - a partitioned, re-runnable path |
| `@dataset().container`, `@dataset().folder`, `@dataset().file_name` | datasets | dataset parameters used in the location -> one dataset serves all files |
| `@linkedService().storage_account` | linked service | one ADLS linked service for dev and prod |
| `@concat(pipeline().parameters.notebook_root, '/01_bronze_ingestion')` | Databricks activity | notebook path from a parameter (dev vs prod workspace folders) |
| `@pipeline().RunId`, `@pipeline().Pipeline` | SP / Web activities | audit values written to the run log / alert |
| `@activity('Execute_Ingest').error.message` | failure handlers | error text stored in `logistics.pipeline_run_log` |
| `@concat('TRUNCATE TABLE [', item().sql_schema, '].[', item().sql_table, ']')` | Gold Copy `preCopyScript` | truncates the target before loading; Gold tables have no FKs |
| `@pipeline().parameters.gold_tables` | ForEach `items` | array parameter; add a table by editing the list, not the pipeline |
Single quotes inside expression strings are escaped by doubling (`''`).

## Retry, timeout and failure handling
* Policy on Copy / notebook activities: `retry = 2`, `retryIntervalInSeconds = 60`, `timeout = 0.01:00:00` (1 h; 30 min for Copy).
* Stored-procedure logging activities retry twice with 30 s interval.
* Failure path = webhook (`alert_webhook_url`) -> SQL log row (`Failed`) -> `Fail` activity with a custom error code (`LOGISTICS_INGEST_FAILED`, ...) so the run is marked **Failed** in Monitor.
* Notebook 06 raises an exception if any Silver data-quality check fails, so bad data never reaches Azure SQL (the Execute step fails before `PL_Load_Gold_To_SQL`).

## Logging and monitoring
* **Run log table:** `logistics.pipeline_run_log` (Started / Succeeded / Failed + message).
* **ADF Monitor:** per-activity input/output, rows copied, duration.
* **Azure Monitor:** diagnostic logs to Log Analytics, failed-run alert rule (see `azure_setup_guide.md`).
* **Databricks:** notebook `logging` output in the run page; data-quality results in `gold/data_quality_report`.
* **Local simulation:** `logs/pipeline.log`, `logs/run_<id>.json`, `results/pipeline_run_log.csv`; retry demo in `results/pipeline_run_log_retry_demo.csv`.

## Transformation flow (Databricks)
![Databricks flow](../architecture/databricks_transformation_flow.png)

| Notebook | Input -> Output | Key logic |
|---|---|---|
| 01 bronze_ingestion | raw CSV -> `bronze/tables/*` | all-string read, audit columns, `md5` row hash, collect-then-fail error handling |
| 02 silver_data_cleaning | Bronze -> `silver/tables/*`, `rejected_records` | `coalesce(when(...))` ordered rules, `row_number` de-dup, broadcast FK checks, derived status, cost imputation |
| 03 shipment_transformation | Silver -> `shipment_enriched` | broadcast joins, groupBy event summary, `row_number` last event, delay bucket, SLA breach |
| 04 inventory_transformation | Silver -> `inventory_enriched` | stock status, `lag` (prev month), `max` window (latest snapshot) |
| 05 gold_analytics | enriched -> 8 `gold/*` tables | conditional aggregation, ranking, `lag` month-over-month, running sum, KPI table |
| 06 data_quality_validation | Bronze + Silver -> `gold/data_quality_report` | check suite, reconciliation, gate that fails the job |

## Local pandas implementation vs Databricks notebooks
Same rules, same column names and the same output tables. The pandas version is what produced the numbers in this repository; the notebooks have not been run on Spark here. Expect tiny differences only where Spark uses approximate functions (e.g. `approxQuantile` for the median weight used to impute missing weights).
