# Azure Resource Configuration Guide

> These steps were **not executed** for this delivery (no Azure subscription). Names in `<angle brackets>` are placeholders you choose. Never commit secrets.

## 1. Resource group and naming
`az group create -n rg-logistics-dev -l <region>`
Suggested names: `stlogisticsdev001` (storage), `adf-logistics-dev`, `dbw-logistics-dev`, `sql-logistics-dev` / `logisticsdb`, `kv-logistics-dev`, `log-logistics-dev`.

## 2. ADLS Gen2
* Storage account, *Enable hierarchical namespace = on*, LRS is enough for dev.
* Containers: `landing`, `bronze`, `silver`, `gold`, `config`.
* Upload: `data/raw/shipments.csv` and `shipment_tracking_events.csv` -> `landing/TMS/`; `warehouses.csv`, `inventory.csv` -> `landing/WMS/`; `transportation_partners.csv` -> `landing/ERP/`; `customers.csv` -> `landing/CRM/`; `adf/parameters/source_config.json` -> `config/`.
* The folder name equals `source_system` in `source_config.json` (the ADF Copy source folder is `@item().source_system`).

## 3. Azure SQL Database
1. Create logical server + database `logisticsdb` (Standard S1 is fine).
2. Firewall: allow your client IP; enable *Allow Azure services* (or use a private endpoint in production).
3. Run, in order: `sql/01_database_setup.sql`, `02_table_creation.sql`, `04_stored_procedures.sql`.
4. Create a contained user for the ADF managed identity (commented block in `01_database_setup.sql`).

## 4. Key Vault
* Create the vault (RBAC model). Secrets: `sql-admin-password`, `databricks-access-token`.
* Grant the ADF managed identity role **Key Vault Secrets User**.
* The linked services reference these secrets by name - no password appears in JSON.

## 5. Databricks
* Create a workspace; generate a personal access token (or use Microsoft Entra ID token authentication in production) and store it in Key Vault.
* Import `notebooks/databricks_source/*.py` into `/Shared/logistics` (Workspace -> Import).
* Storage access: assign the Databricks access connector / service principal *Storage Blob Data Contributor* on the storage account (Unity Catalog external locations are the recommended approach).
* ADF creates a job cluster per notebook run (`newClusterVersion` etc. in `LS_AzureDatabricks.json`; adjust node type to what your region offers).

## 6. Data Factory
1. Create the factory (system-assigned managed identity on).
2. Grant it: *Storage Blob Data Contributor* on the storage account, *Key Vault Secrets User*.
3. Configure Git integration (GitHub) and use `adf/` as the root folder, or paste the JSON via *Manage -> ARM template / Code view*.
4. Replace placeholders: `<storage-account-name>`, `<your-key-vault-name>`, `<sql-server-name>`, `<database-name>`, `<sql-admin-user>`, `<databricks-workspace-id>.<region>`, `<your-alert-webhook-url>` (Teams/Slack/Logic App URL).
5. Publish. Trigger `PL_Master_Logistics` manually with `run_date = 2026-06-30` (debug run), then start `tr_daily_logistics`.

## 7. Azure Monitor / Log Analytics
* Factory -> *Diagnostic settings* -> send `PipelineRuns`, `ActivityRuns`, `TriggerRuns` to the Log Analytics workspace (resource-specific tables).
* Alert rule on metric `Failed pipeline runs metrics` (threshold > 0, 5 min window) with an action group (email/Teams).
* Example KQL: `ADFPipelineRun | where Status == "Failed" | project TimeGenerated, PipelineName, RunId, FailureType | order by TimeGenerated desc`.

## 8. Validate the deployment
| Check | Expected |
|---|---|
| ADF Monitor | `PL_Master_Logistics` Succeeded; ForEach copies 6 files |
| ADLS | `bronze/tables/*`, `silver/tables/*`, `gold/*` folders populated |
| Azure SQL | `SELECT COUNT(*) FROM logistics.fact_shipment` = 11,667 for the sample data |
| `logistics.pipeline_run_log` | `Started` then `Succeeded` rows for the run |
| Failure test | rename a landing file -> pipeline fails, alert fires, `Failed` row logged |

## 9. Cost control
Stop the Databricks cluster (job clusters auto-terminate); pause/scale down SQL when idle; delete the resource group when finished: `az group delete -n rg-logistics-dev`.
