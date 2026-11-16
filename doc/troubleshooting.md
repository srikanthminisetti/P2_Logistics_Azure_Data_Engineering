# Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ModuleNotFoundError: src` | running from the wrong folder | run commands from the project root (`python -m src.run_pipeline`) |
| `FileNotFoundError: data/raw/...` | data not generated | `python -m src.data_generation.generate_data` or omit `--skip-generate` |
| Pipeline stops with "Silver data-quality checks FAILED" | a new cleaning rule/regression | read the table printed in the error; fix the rule in `silver_cleaning.py`; this gate is intentional |
| `pytest` not found | not installed | `pip install pytest`, or `python scripts/run_tests.py` (built-in runner) |
| Parquet files missing locally | `pyarrow` not installed | optional - CSV outputs are always written; `pip install pyarrow` for Parquet |
| Notebook: `Path does not exist` (local) | Jupyter started outside the project | start Jupyter from the project root or `notebooks/` folder |
| Notebook: `NameError: dbutils` | running outside Databricks | expected; `get_param` falls back to defaults (local mode) |
| Dates become NULL in Spark | non-ISO format with a different locale, or ANSI mode on | keep `spark.sql.ansi.enabled=false` and `timeParserPolicy=CORRECTED` (set in the setup cell) |
| ADF: *Linked service parameter not found* | dataset not passing `storage_account` | datasets pass `@dataset().storage_account` to `LS_ADLS_Gen2`; supply it from the pipeline |
| ADF Copy: `The specified path does not exist` | file not in `landing/<SYSTEM>/` | folder must equal `source_system` in `source_config.json` |
| ADF: `AuthorizationPermissionMismatch` (ADLS 403) | managed identity lacks the role | assign *Storage Blob Data Contributor* to the factory identity |
| ADF: Key Vault 403 | missing secret permission | assign *Key Vault Secrets User* to the factory identity |
| Databricks activity fails to start | wrong node type / quota | change `newClusterNodeType` for your region or use an existing cluster id |
| SQL: `Cannot truncate table ... referenced by a FOREIGN KEY` | TRUNCATE on dim/fact | use `EXEC logistics.usp_prepare_reload` (DELETE child-first) |
| SQL: Copy `FK violation` | facts loaded before dimensions | keep the order: dimension -> fact -> gold (already in `PL_Load_Gold_To_SQL`) |
| Alert webhook not firing | placeholder URL left in parameters | set `alert_webhook_url` to a real Teams/Slack/Logic App URL |
| `unbalanced expression` from the ADF validator | typo in a dynamic expression | fix parentheses / escape `'` as `''` in `scripts/generate_adf_json.py`, regenerate |

## Useful commands
```bash
python -m src.run_pipeline --skip-generate      # re-run without regenerating raw data
python -m src.validation.validate_adf_json      # ADF static validation
python -m src.validation.lint_sql               # SQL static lint
python scripts/generate_adf_json.py             # regenerate ADF JSON
python scripts/build_notebooks.py               # rebuild .ipynb from notebooks_src/
```
