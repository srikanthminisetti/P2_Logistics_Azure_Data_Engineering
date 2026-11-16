# Step-by-Step Implementation Guide (beginner friendly)

## Part A - Run everything locally (no Azure needed)
1. **Install** Python 3.10+ and run `pip install pandas numpy matplotlib pytest`.
2. **Generate data:** `python -m src.data_generation.generate_data` -> six CSVs in `data/raw/`. Open `shipments.csv` and look for `15/03/2026` style dates and duplicate ids.
3. **Run the whole pipeline:** `python -m src.run_pipeline`. Watch the stages: bronze, silver, shipment_transformation, inventory_transformation, gold, data_quality, reports.
4. **Inspect each layer:** `data/bronze` (audit columns), `data/processed` (clean + `rejected_records.csv`), `data/gold`.
5. **Read the answers:** `results/execution_summary.md`, charts in `results/performance_charts/`.
6. **Run tests:** `python scripts/run_tests.py` (or `pytest -v`).
7. **Break something on purpose:** `python -m src.run_pipeline --skip-generate --simulate-failure silver` and read `results/pipeline_run_log.csv`.

## Part B - Understand the code
| File | Read it to learn |
|---|---|
| `src/utils/date_utils.py` | how mixed date formats are standardised safely |
| `src/ingestion/bronze_ingestion.py` | audit columns and the "never change raw data" rule |
| `src/transformations/silver_cleaning.py` | rule-based quarantine (`apply_rules`), PK/FK checks, derived status |
| `src/transformations/gold_analytics.py` | KPI definitions, ranking, `lag` / cumulative sums |
| `src/validation/data_quality.py` | one small function per check, rolled into a report |
| `notebooks_src/*.py` | the same logic in PySpark (Databricks) |

## Part C - Move to Azure
1. Follow `docs/azure_setup_guide.md` to create resources and permissions.
2. Run `sql/01_database_setup.sql` then `sql/02_table_creation.sql` and `sql/04_stored_procedures.sql`.
3. Upload sample data to ADLS, import notebooks, replace placeholders in `adf/`, publish and run `PL_Master_Logistics`.
4. Verify: row counts in `logistics.*` tables should match `results/execution_summary.md`; run `sql/03_analytics_queries.sql`.
5. Capture screenshots (`screenshots/README.md`).

## Part D - Extend the project
* Add a new source table: add one row to `adf/parameters/source_config.json` (ingestion is metadata-driven) and a cleaning function.
* Add a Gold table: write it in `05_gold_analytics`, add it to the `gold_tables` pipeline parameter and a SQL table.
* Regenerate ADF JSON after editing `scripts/generate_adf_json.py`; rebuild notebooks with `python scripts/build_notebooks.py`.
