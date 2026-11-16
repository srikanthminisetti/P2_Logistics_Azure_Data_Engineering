# Execution Summary

_Generated 2026-11-15 11:58 UTC by `python -m src.run_pipeline`._

> **Execution environment: LOCAL (pandas reference implementation).** Nothing in this summary was executed in Azure. ADF JSON, Databricks notebooks and Azure SQL scripts are deployment-ready templates that have **not** been deployed or run in an Azure subscription. See `docs/azure_setup_guide.md`.

## 1. Record counts per layer

| Table | Bronze rows | Silver rows | Quarantined |
|---|---:|---:|---:|
| customers | 303 | 300 | 3 |
| warehouses | 13 | 12 | 1 |
| transportation_partners | 9 | 8 | 1 |
| shipments | 12,144 | 11,667 | 477 |
| shipment_tracking_events | 79,070 | 75,037 | 4,033 |
| inventory | 5,806 | 5,675 | 131 |

Bronze = Silver + Quarantined for every table (verified by the reconciliation checks).

## 2. Answers to the 10 business questions (from Gold)

1. **Total shipments:** 11,667
2. **Status:** Delivered on time 8,958, Delayed 2,206, In Transit 159, Cancelled 344
3. **Highest-delay partner:** SwiftHaul Logistics (TP02) - 32.09% of completed deliveries delayed (next: PrimeMove Transport 25.89%)
4. **Average delivery time:** 5.16 days (ship date to delivery)
5. **Low-inventory warehouses:** Newark (WH05, 55.0% SKUs low), Phoenix (WH09, 47.5% SKUs low), Dallas (WH02, 42.5% SKUs low), Kansas City (WH11, 42.11% SKUs low)
6. **On-time delivery rate:** 80.24%
7. **Most-delayed route:** WH01-SFO Chicago -> San Francisco (26 delayed of 88)
8. **Transportation cost:** total $26,144,273.97; peak month 2025-12 ($3,285,709.39)
9. **Customer with most delayed deliveries:** Taylor Online (C0100) - 16 delayed
10. **Inventory (latest snapshot):** 178,567 units worth $38,637,190.18 across 12 warehouses

## 3. Data-quality results

* Source (Bronze) checks that detected issues: **30** of 45 - expected, the data is intentionally dirty.
* Silver checks passed: **43** of 45 (failing Silver checks abort the pipeline).
* Reconciliation (bronze = silver + rejected): all tables PASS

Top source issues found:

| Table | Check | Column | Failed rows |
|---|---|---|---:|
| shipment_tracking_events | unique_key | event_id | 782 |
| shipments | not_null | actual_delivery_date | 560 |
| shipment_tracking_events | not_null | event_timestamp | 392 |
| shipment_tracking_events | referential_integrity->shipments | shipment_id | 316 |
| shipment_tracking_events | valid_dates | event_timestamp | 237 |
| shipment_tracking_events | allowed_values | event_type | 156 |
| shipments | not_null | shipping_cost | 145 |
| shipments | unique_key | shipment_id | 144 |
| shipments | delivery_date_order | actual_delivery_date | 61 |
| shipments | not_null | origin_warehouse_id | 60 |
| shipments | allowed_values | shipment_status | 48 |
| inventory | unique_key | inventory_id | 46 |

## 4. Pipeline stage log (local run)

| Stage | Status | Attempts | Seconds |
|---|---|---:|---:|
| bronze | Succeeded | 1 | 3.85 |
| silver | Succeeded | 1 | 1.11 |
| shipment_transformation | Succeeded | 1 | 0.2 |
| inventory_transformation | Succeeded | 1 | 0.04 |
| gold | Succeeded | 1 | 0.15 |
| data_quality | Succeeded | 1 | 0.96 |

## 5. Generated artefacts

* `data/bronze`, `data/processed` (Silver), `data/gold` - CSV (+ Parquet when `pyarrow` is installed)
* `results/data_quality_report.csv`, `missing_value_analysis.csv`, `rejected_records_summary.csv`
* `results/shipment_analysis.csv`, `inventory_analysis.csv`, `business_kpi_answers.csv`
* `results/performance_charts/*.png` (8 charts)
