# Interview Questions and Answers

## Architecture and design
**1. Why a Bronze / Silver / Gold architecture?**
It separates concerns: Bronze keeps raw data for replay and audit, Silver holds cleaned and validated data that is reusable by many consumers, Gold holds business-specific aggregates. A bug in cleaning can be fixed and Silver rebuilt from Bronze without re-extracting from source systems.

**2. Why quarantine bad rows instead of deleting them?**
Deleting hides problems and makes counts impossible to reconcile. With a quarantine table (`rejected_records`, one reason per row) the team can fix the source, report quality by system and prove `bronze = silver + rejected`.

**3. Why derive `shipment_status` from the dates?**
In the data, ~0.4 % of rows are labelled Delivered although delivery was after the promise. The dates are the facts; the label is a human/system opinion. Deriving the status gives consistent KPIs, and a `status_corrected` flag keeps it auditable.

**4. Why Parquet in the lake and a separate Azure SQL layer?**
The lake is cheap, scalable storage for all history and for Spark. Azure SQL serves low-latency, SQL-based reporting with constraints and indexes. ADF copies Gold into SQL.

**5. Delta vs Parquet?**
Delta adds ACID transactions, `MERGE`, time travel and schema enforcement. I used Parquet so ADF Copy can read folders directly and the sample stays simple; moving to Delta is listed as an improvement (use `MERGE` for incremental loads).

## Azure Data Factory
**6. What does "metadata-driven" mean here?**
A config file (`source_config.json`) lists the tables. A Lookup reads it and a ForEach copies each one using parameterised datasets. Adding a table needs a config row, not a new pipeline.

**7. Explain `@item()` and `@activity('Lookup').output.value`.**
The Lookup returns an array; `output.value` is passed to ForEach `items`; inside the loop `@item()` is the current element, e.g. `@item().file_name`.

**8. How do you handle failures in ADF?**
Retry on each activity (`retry=2`, 60 s). Failure dependency paths (`Failed`) trigger a Web activity alert, a stored-procedure log row, and a `Fail` activity so the run shows Failed. Because multiple dependencies are ANDed, each stage has its own handler.

**9. Why `DELETE` in a stored procedure instead of `TRUNCATE` in the Copy pre-copy script for facts?**
`TRUNCATE` is not allowed on tables referenced by foreign keys. `usp_prepare_reload` deletes child-first inside a transaction, then dimensions and facts are loaded in order. Gold tables have no FKs, so they use TRUNCATE.

**10. How are secrets handled?**
No secrets in JSON. SQL password and Databricks token are Key Vault secrets referenced by linked services; ADLS access uses the factory's managed identity. `.gitignore` and a repo secret-scan test guard against accidents.

**11. How would you make the pipeline incremental?**
Use a watermark (e.g. `updated_at` or `ingest_date`) stored in a control table, Lookup it, filter the source query, write to Delta and `MERGE` on the business key; keep a full-reload parameter for backfills.

## PySpark
**12. How do you de-duplicate while keeping the first record?**
`row_number().over(Window.partitionBy(key).orderBy("_row_id"))`; keep rank 1 and quarantine the rest.

**13. How do you validate foreign keys efficiently?**
Broadcast the small parent key set and left-join; rows where the parent key is null are orphans. `left_anti` join is the equivalent for counting.

**14. Why `spark.sql.ansi.enabled=false` in the notebooks?**
So invalid dates/numbers become NULL (and are then flagged) instead of throwing, which fits the "detect and quarantine" approach.

**15. Where did you use window functions?**
Duplicate ranking, latest tracking event per shipment, `lag` for previous-month inventory and month-over-month cost, `max` over warehouse for latest snapshot, running total of cost, ranking in Gold.

## SQL
**16. Write the on-time percentage.**
`100.0 * SUM(CASE WHEN shipment_status='Delivered' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END),0)` - In Transit and Cancelled are excluded because their outcome is not known / not applicable.

**17. RANK vs DENSE_RANK vs ROW_NUMBER?**
RANK leaves gaps after ties, DENSE_RANK does not, ROW_NUMBER gives unique numbers. Gold tables use ROW_NUMBER for unique ranks; Q09 uses DENSE_RANK so tied customers share a rank.

**18. How do you find the latest snapshot per warehouse?**
A CTE with `MAX(snapshot_date)` grouped by warehouse joined back, or `ROW_NUMBER() ... ORDER BY snapshot_date DESC = 1`.

## Data quality and testing
**19. Which checks did you implement?**
Nulls, duplicate keys, invalid dates, referential integrity, allowed values, negative quantities, delivery-before-shipment, numeric type checks and Bronze/Silver reconciliation; Silver ERROR failures abort the pipeline.

**20. How is the code tested?**
Unit tests build tiny DataFrames with known defects and assert the exact rejection reason; integration tests assert Bronze contains the planted issues, Silver passes all checks, counts reconcile, Gold totals match Silver; further tests validate ADF JSON, SQL lint and notebook syntax.

**21. What would you do differently in production?**
Delta Lake + Unity Catalog, CI/CD (ARM/Bicep or Terraform), private endpoints, incremental loads, expectations framework (Great Expectations / DLT), alerting on data-quality trends, cost monitoring.

**22. What are the limitations of this delivery?**
The ADF, Databricks and Azure SQL parts were validated statically but not executed in Azure; the results come from the pandas reference implementation. A real deployment may need adjustments (cluster node types, permissions, network rules).
