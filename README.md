# End-to-End Logistics & Supply Chain Data Engineering Pipeline on Azure

> Bronze -> Silver -> Gold medallion pipeline for shipment, delivery-tracking, warehouse-inventory and transport-cost data.
> **Stack:** Python - SQL - Azure Data Factory - ADLS Gen2 - Azure Databricks (PySpark) - Azure SQL Database - Key Vault - Azure Monitor - Git/GitHub

![Architecture](architecture/architecture_diagram.png)

## IMPORTANT - what was actually executed

| Component | Status in this repository |
|---|---|
| Data generation, Bronze/Silver/Gold pipeline (pandas reference implementation), data-quality checks, charts, reports | **Executed locally** - outputs are in `data/`, `results/`, `logs/` |
| Automated tests (40) | **Executed locally** (with `data/processed` present) - 40/40 pass (`results/test_report.md`). Run with the built-in fallback runner because `pytest` could not be installed offline; the tests are plain `assert` functions and run unchanged under `pytest -v` |
| ADF JSON (4 pipelines, 5 datasets, 4 linked services, 1 trigger) | **Statically validated** (valid JSON, references, dependencies, expressions, no secrets). **Not deployed or run in Azure** |
| Databricks / PySpark notebooks 01-06 | **Syntax-checked only. Not executed** - PySpark was not available in the build environment. "Expected output" cells show values from the pandas reference run of the same logic |
| Azure SQL scripts (24 queries, 5 procedures) | **Statically linted only. Not executed** on Azure SQL Database |
| Azure resources (storage, Databricks, SQL, Key Vault, Monitor) | **Not created.** `docs/azure_setup_guide.md` explains how |

Nothing in this project claims a real Azure run. Deploy it with your own subscription following the guides.

## 1. Business problem
A logistics company ships goods from 12 warehouses to 24 US cities through 8 carriers (road, rail, air). Data lives in separate systems - TMS (shipments & tracking events), WMS (warehouses & inventory), ERP (carriers), CRM (customers) - with duplicates, missing values, mixed date formats and broken keys. Operations needs reliable answers about delivery performance, carrier delays, stock levels and transportation cost.

## 2. Objectives
* Ingest six source datasets with a metadata-driven, parameterised ADF pipeline.
* Build Bronze / Silver / Gold layers with PySpark; quarantine bad records instead of losing them.
* Enforce primary/foreign keys and business rules; measure and report data quality.
* Serve analytics in Azure SQL with 24 queries and stored procedures.
* Answer the 10 business questions below.

## 3. Business questions answered (local reference run)
| # | Question | Answer |
|---|---|---|
| 1 | Total shipments | **11,667** |
| 2 | Delivered / delayed / in transit / cancelled | Delivered on time **8,958** - Delayed **2,206** - In Transit **159** - Cancelled **344** |
| 3 | Partners with highest delays | **SwiftHaul Logistics (TP02) 32.09 %**, then PrimeMove Transport 25.89 %, RailConnect Cargo 23.26 % |
| 4 | Average delivery time | **5.16 days** |
| 5 | Warehouses with low inventory | **Newark (55 % of SKUs low), Phoenix (47.5 %), Dallas (42.5 %), Kansas City (42.1 %)** |
| 6 | On-time delivery % | **80.24 %** |
| 7 | Routes with most delays | **Chicago -> San Francisco (26 delayed of 88)**, Atlanta -> Phoenix (22), Los Angeles -> Chicago (19) |
| 8 | Monthly transportation cost | total **$26.14 M**; peak **Dec 2025 $3.29 M** (Nov-Dec peak season) |
| 9 | Customers with most delayed deliveries | **Taylor Online (C0100) 16**, Wilson Online (C0202) 16, Williams Traders (C0297) 14 |
| 10 | Inventory levels across warehouses | **178,567 units, $38.64 M** at the 30 Jun 2026 snapshot; Los Angeles highest (19,933 units), Newark lowest (8,856) |

Full detail: `results/execution_summary.md`, `results/business_kpi_answers.csv`, charts in `results/performance_charts/`.

![Partner delay rate](results/performance_charts/03_partner_delay_rate.png)
![Monthly volume vs on-time](results/performance_charts/02_monthly_shipments_on_time.png)

## 4. Technology stack
Python 3.10+, pandas, matplotlib, pytest - PySpark 3.5 (Databricks) - T-SQL (Azure SQL Database) - Azure Data Factory - ADLS Gen2 - Azure Key Vault - Azure Monitor - Git.

## 5. Folder structure
```
P2_Logistics_Azure_Data_Engineering/
|-- README.md  requirements.txt  .gitignore  LICENSE
|-- architecture/   architecture.md + 3 diagrams (architecture, ADF orchestration, Databricks flow)
|-- adf/            linkedServices/, datasets/, pipelines/, triggers/, parameters/
|-- notebooks/      01_bronze_ingestion ... 06_data_quality_validation (.ipynb) + databricks_source/ (.py versions for Databricks import)
|-- notebooks_src/  notebook templates (shared setup in _common.py)
|-- sql/            01_database_setup, 02_table_creation, 03_analytics_queries (24), 04_stored_procedures, 05_data_loading_examples
|-- src/
|   |-- data_generation/  ingestion/  transformations/  validation/  utils/  visualization/
|   |-- run_pipeline.py   run_summary.py
|-- tests/          test_data_quality.py, test_transformations.py, test_project_assets.py
|-- scripts/        generate_adf_json.py, build_notebooks.py, run_tests.py, make_previews.py
|-- data/
|   |-- raw/        6 source CSVs (generated, intentionally dirty)
|   |-- gold/       8 analytics tables
|   |-- data_dictionary.md, sample_previews.md
|   |-- (bronze/ and processed/ are NOT committed - created by running the pipeline)
|-- results/        execution_summary.md, test_report.md, data_quality_report.csv, shipment_analysis.csv,
|                   inventory_analysis.csv, performance_charts/ (8 PNG)
|-- docs/           overview, business requirements, implementation guide, Azure setup, pipeline explanation, interview Q&A, troubleshooting
|-- screenshots/    README.md (what to capture after deploying to Azure)
```

## 6. Datasets
Six interconnected CSVs (12,144 shipment rows, 79,070 tracking events, 5,806 inventory snapshots, 303 customers, 13 warehouses, 9 partners) covering Jul 2025 - Jun 2026. Keys: `customer_id`, `warehouse_id`, `partner_id`, `shipment_id`. Problems planted on purpose: duplicates, nulls, mixed date formats, orphan keys, invalid statuses, negative stock, delivery-before-shipment. See `data/data_dictionary.md` and `data/sample_previews.md`.

## 7. Pipeline workflow
1. **Trigger** `tr_daily_logistics` (02:00 UTC) -> `PL_Master_Logistics(run_date)`.
2. **Ingest** `PL_Ingest_Raw_To_Bronze`: Lookup config -> ForEach table -> Copy `landing` -> `bronze/raw_files/<table>/ingest_date=<run_date>/`.
3. **Transform** `PL_Run_Databricks_Transformations`: 01 Bronze -> 02 Silver -> 03 shipments || 04 inventory -> 05 Gold -> 06 quality gate.
4. **Serve** `PL_Load_Gold_To_SQL`: clear -> dimensions -> facts -> 8 Gold tables in Azure SQL.
5. **Log / alert** run-log rows in SQL, webhook + `Fail` activity on any failure, retry = 2 on every Copy / notebook.
Details and every dynamic expression: `docs/pipeline_explanation.md`.

## 8. Transformation logic (highlights)
* **Bronze:** all columns kept as strings + `_ingestion_timestamp`, `_batch_id`, `_source_system`, `_source_file`, `_row_id`, `_record_hash`.
* **Silver:** trim/blank->NULL; mixed dates -> DATE; vocabulary normalisation; duplicates (keep first); PK/FK validation; quarantine with a single reason; status **derived from dates**; missing cost imputed (`distance x contract rate x priority factor`) and flagged; reconciliation `bronze = silver + rejected`.
* **Enrichment:** broadcast joins to dimensions; tracking-event summary with window function (`row_number`) for last event; delay bucket; SLA breach flag; inventory status, `lag()` trend, latest-snapshot window.
* **Gold:** 8 tables with ranking (`row_number`), `lag()` month-over-month cost, running total, conditional aggregates.

## 9. SQL analysis
`sql/03_analytics_queries.sql` has 24 queries (joins, GROUP BY/HAVING, subqueries, CTEs, `RANK`/`DENSE_RANK`/`ROW_NUMBER`, `LAG`, moving average, `PERCENTILE_CONT`, CASE, `DATEDIFF`/`DATEPART`, ROLLUP). `sql/04_stored_procedures.sql` has 5 procedures (run log, FK-safe reload, partner KPIs, low-stock alert, monthly-cost refresh).

## 10. Data-quality checks
Null values, duplicate keys, invalid dates, missing warehouse ids, invalid statuses, negative inventory, delivery before shipment, referential integrity, unexpected data types, reconciliation. Latest local run: **45 Bronze checks (30 detect issues - expected), 45 Silver checks (43 pass, 2 are WARN-level nulls that are legal), 6 reconciliation checks pass.** See `results/data_quality_report.csv`, `rejected_records_summary.csv`, `missing_value_analysis.csv`.

## 11. How to run locally
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install pandas numpy matplotlib pytest             # or: pip install -r requirements.txt
python -m src.run_pipeline                             # generate data -> Bronze -> Silver -> Gold -> DQ -> charts -> reports
python scripts/run_tests.py                            # or: pytest -v   (run AFTER the pipeline)
python -m src.validation.validate_adf_json             # static ADF validation
python -m src.validation.lint_sql                      # static SQL lint
python -m src.run_pipeline --skip-generate --simulate-failure silver   # watch retry behaviour
```
`data/bronze/` and `data/processed/` (Silver layer) are not stored in GitHub; `python -m src.run_pipeline` creates them (the tests need them, so run the pipeline before the tests).\n\nOptional: `pip install pyspark jupyter` and open `notebooks/*.ipynb` - with no widgets present they run in *local mode* (read `data/raw`, write `data/*_spark/`).

## 12. Azure deployment (summary)
1. Create resource group, ADLS Gen2 (containers `landing`, `bronze`, `silver`, `gold`, `config`), Key Vault, Databricks workspace, Azure SQL Database, Data Factory, Log Analytics.
2. Grant ADF managed identity *Storage Blob Data Contributor* and Key Vault *get/list secrets*; store `sql-admin-password` and `databricks-access-token` as secrets.
3. Upload `data/raw/*.csv` to `landing/<TMS|WMS|ERP|CRM>/` and `adf/parameters/source_config.json` to `config/`.
4. Import `notebooks/databricks_source/*.py` (or the `.ipynb` files) into `/Shared/logistics`.
5. Run `sql/01..04` on Azure SQL.
6. Replace `<placeholders>` in `adf/`, publish via ADF Git integration, run `PL_Master_Logistics`.
Full step-by-step: `docs/azure_setup_guide.md`.

## 13. Expected outputs
`data/gold/*.csv` (8 tables), `results/execution_summary.md`, `results/data_quality_report.csv`, `results/shipment_analysis.csv`, `results/inventory_analysis.csv`, 8 PNG charts, `results/test_report.md`.

## 14. Future improvements
Delta Lake with `MERGE` for incremental loads and time travel - Auto Loader / event-driven triggers - Great Expectations or Delta expectations - Unity Catalog governance - Azure DevOps/GitHub Actions CI/CD with ARM/Bicep - Power BI semantic model - near-real-time tracking events with Event Hubs + Structured Streaming - carrier ETA prediction model.


