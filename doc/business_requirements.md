# Business Requirements

| # | Business question | Gold table / SQL query | Metric definition | Local result |
|---|---|---|---|---|
| 1 | Total number of shipments? | `gold_kpi_summary`, Q01 | count of valid shipments after Silver cleaning | 11,667 |
| 2 | Delivered, delayed, in transit, cancelled? | `gold_shipment_performance_summary`, Q02 | status derived from dates | 8,958 / 2,206 / 159 / 344 |
| 3 | Partners with highest delays? | `gold_partner_performance`, Q03 | delayed / (delivered + delayed) | SwiftHaul 32.09 % |
| 4 | Average delivery time? | `gold_kpi_summary`, Q04 | mean(actual delivery - ship date) over completed shipments | 5.16 days |
| 5 | Warehouses with low inventory? | `gold_warehouse_inventory_summary`, Q05, `usp_low_stock_alert` | SKUs with qty <= reorder level, latest snapshot | Newark, Phoenix, Dallas, Kansas City |
| 6 | % delivered on time? | `gold_kpi_summary`, Q06 | delivered / (delivered + delayed) | 80.24 % |
| 7 | Routes with most delays? | `gold_route_performance`, Q07, Q14 | count of delayed shipments per route | WH01-SFO (26) |
| 8 | Monthly transportation cost? | `gold_monthly_cost_analysis`, Q08, Q17 | sum(shipping_cost) by ship month, split by mode | $26.14 M total |
| 9 | Customers with most delayed deliveries? | `gold_customer_delay_analysis`, Q09 | delayed shipments per customer | Taylor Online (16) |
| 10 | Inventory levels across warehouses? | `gold_warehouse_inventory_summary`, Q10 | units and value at latest snapshot | 178,567 units |

## Non-functional requirements
| Area | Requirement | How it is met |
|---|---|---|
| Reliability | transient failures must not require manual reruns | retry = 2 on every Copy / notebook / SP activity |
| Observability | every run auditable | `logistics.pipeline_run_log`, ADF Monitor, Azure Monitor alerts, local `logs/` |
| Data quality | bad data visible, never silently lost | quarantine table, 45+ checks, reconciliation |
| Security | no secrets in code | Key Vault references, managed identity, `.gitignore`, secret-scan test |
| Re-runnability | same input -> same output | seeded generator, truncate-and-load, idempotent notebooks (`overwrite`) |
| Maintainability | add a table without code changes | metadata-driven ingestion (`source_config.json`) |
