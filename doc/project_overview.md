# Project Overview

**Title:** End-to-End Logistics & Supply Chain Data Engineering Pipeline Using Azure
**Level:** easy-to-medium - suitable for a junior/mid data-engineer portfolio.

## The story
*FreightFlow* (fictional) moves goods from 12 US distribution centres to 24 cities using 8 carriers. Four systems hold the data (TMS, WMS, ERP, CRM). Reports disagree because each extract has duplicates, mixed date formats, missing keys and wrong statuses. The data team builds a lakehouse pipeline so operations, finance and customer-service teams get one trusted set of numbers.

## What you build
1. A reproducible **data generator** (`src/data_generation`) producing six related, imperfect datasets.
2. A **medallion pipeline**: Bronze (raw + audit) -> Silver (clean, validated, quarantine) -> Gold (8 analytics tables).
3. **Azure Data Factory** pipelines to orchestrate ingestion, notebooks, loading and alerting.
4. **PySpark notebooks** for Databricks (and a pandas reference implementation that runs anywhere).
5. An **Azure SQL** serving layer with 24 queries and stored procedures.
6. **Data-quality framework + tests** and generated reports / charts.

## Skills demonstrated
Metadata-driven orchestration - medallion modelling - PySpark joins, windows and aggregations - data-quality engineering - PK/FK enforcement - T-SQL analytics - secure configuration (Key Vault, managed identity) - testing - documentation.

## Honest scope statement
Logic was executed and tested locally with pandas. ADF JSON, notebooks and SQL are deployment-ready templates that were statically validated but not run in Azure (see README, "What was actually executed").

## Reading order
README -> `architecture/architecture.md` -> `data/data_dictionary.md` -> `docs/pipeline_explanation.md` -> `results/execution_summary.md`.
