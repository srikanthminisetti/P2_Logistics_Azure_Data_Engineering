/* =====================================================================================
   05_data_loading_examples.sql  -  how data gets into Azure SQL (examples / templates)
   In the automated pipeline ADF (PL_Load_Gold_To_SQL) loads Parquet from ADLS. These
   examples show manual alternatives and tiny INSERT samples for testing the schema.
   Placeholders: <storage-account>, <container>, <SAS-token> - never commit real values.
   ===================================================================================== */

/* --- A. Minimal sample rows (valid against all constraints) --- */
INSERT INTO logistics.dim_customer (customer_id, customer_name, segment, city, state, country, email, email_status, registration_date)
VALUES ('C0001', N'Sample Retail Group', 'Retail', N'Chicago', 'IL', 'USA', N'contact1@sample.com', 'Valid', '2020-01-15');
INSERT INTO logistics.dim_warehouse (warehouse_id, warehouse_name, city, state, region, capacity_units, manager_name, opened_date)
VALUES ('WH01', N'Chicago Distribution Center', N'Chicago', 'IL', 'Midwest', 80000, N'Pat Lee', '2015-03-01');
INSERT INTO logistics.dim_partner (partner_id, partner_name, transport_mode, contact_email, contract_rate_per_km, sla_days, contract_start_date, is_active)
VALUES ('TP01', N'BlueLine Freight', 'Road', N'ops@blueline.com', 0.92, 5, '2023-01-01', 1);
INSERT INTO logistics.fact_shipment (shipment_id, customer_id, origin_warehouse_id, destination_city, destination_state, partner_id, route_id,
            ship_date, promised_delivery_date, actual_delivery_date, shipment_status, weight_kg, distance_km, shipping_cost, priority, ship_month,
            planned_transit_days, delivery_days, delay_days, is_on_time)
VALUES ('SHP0000001', 'C0001', 'WH01', N'Denver', 'CO', 'TP01', 'WH01-DEN', '2026-01-05', '2026-01-08', '2026-01-09', 'Delayed',
        42.5, 1500.0, 1450.00, 'Standard', '2026-01', 3, 4, 1, 0);

/* --- B. BULK INSERT from Azure Blob/ADLS into a staging table (CSV with header) --- */
/*
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong-password-from-key-vault>';
CREATE DATABASE SCOPED CREDENTIAL logistics_sas WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '<SAS-token-without-leading-?>';
CREATE EXTERNAL DATA SOURCE logistics_lake WITH (TYPE = BLOB_STORAGE,
       LOCATION = 'https://<storage-account>.blob.core.windows.net/<container>', CREDENTIAL = logistics_sas);

BULK INSERT staging.shipments_raw
FROM 'gold/gold_kpi_summary.csv'
WITH (DATA_SOURCE = 'logistics_lake', FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a');
*/

/* --- C. Idempotent upsert from a staging table into a dimension (MERGE) --- */
/*
MERGE logistics.dim_customer AS tgt
USING staging.customers_clean AS src ON src.customer_id = tgt.customer_id
WHEN MATCHED THEN UPDATE SET tgt.customer_name = src.customer_name, tgt.segment = src.segment, tgt.email = src.email
WHEN NOT MATCHED BY TARGET THEN INSERT (customer_id, customer_name, segment, city, state, country, email, email_status, registration_date)
     VALUES (src.customer_id, src.customer_name, src.segment, src.city, src.state, src.country, src.email, src.email_status, src.registration_date);
*/

/* --- D. Row-count smoke test after any load --- */
SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM logistics.dim_customer
UNION ALL SELECT 'fact_shipment', COUNT(*) FROM logistics.fact_shipment
UNION ALL SELECT 'fact_inventory_snapshot', COUNT(*) FROM logistics.fact_inventory_snapshot;
