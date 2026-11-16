/* =====================================================================================
   02_table_creation.sql  -  tables, keys, constraints, indexes and a reporting view
   Layout  : 3 dimensions + 3 facts (conformed Silver data)  |  8 Gold tables  |  1 run-log table
   Columns of the Gold tables and fact/dim tables match the Parquet/CSV columns written by the
   notebooks so ADF Copy can map them by name.
   Run after 01_database_setup.sql. Safe to re-run (drops first).
   ===================================================================================== */

/* ---------- drop in dependency order (children first) ---------- */
DROP VIEW  IF EXISTS logistics.vw_shipment_detail;
DROP TABLE IF EXISTS logistics.fact_tracking_event;
DROP TABLE IF EXISTS logistics.fact_inventory_snapshot;
DROP TABLE IF EXISTS logistics.fact_shipment;
DROP TABLE IF EXISTS logistics.dim_customer;
DROP TABLE IF EXISTS logistics.dim_warehouse;
DROP TABLE IF EXISTS logistics.dim_partner;
DROP TABLE IF EXISTS logistics.gold_kpi_summary;
DROP TABLE IF EXISTS logistics.gold_shipment_performance_summary;
DROP TABLE IF EXISTS logistics.gold_delivery_delay_analysis;
DROP TABLE IF EXISTS logistics.gold_customer_delay_analysis;
DROP TABLE IF EXISTS logistics.gold_warehouse_inventory_summary;
DROP TABLE IF EXISTS logistics.gold_partner_performance;
DROP TABLE IF EXISTS logistics.gold_monthly_cost_analysis;
DROP TABLE IF EXISTS logistics.gold_route_performance;
DROP TABLE IF EXISTS logistics.pipeline_run_log;
GO

/* =============================== DIMENSIONS =============================== */
CREATE TABLE logistics.dim_customer (
    customer_id        VARCHAR(10)   NOT NULL,
    customer_name      NVARCHAR(120) NOT NULL,
    segment            VARCHAR(30)   NOT NULL,
    city               NVARCHAR(60)  NULL,
    state              CHAR(2)       NULL,
    country            VARCHAR(30)   NULL,
    email              NVARCHAR(200) NULL,
    email_status       VARCHAR(10)   NULL,           -- Valid / Missing / Invalid
    registration_date  DATE          NULL,
    bronze_batch_id    VARCHAR(30)   NULL,
    CONSTRAINT PK_dim_customer PRIMARY KEY (customer_id)
);

CREATE TABLE logistics.dim_warehouse (
    warehouse_id    VARCHAR(10)   NOT NULL,
    warehouse_name  NVARCHAR(100) NOT NULL,
    city            NVARCHAR(60)  NOT NULL,
    state           CHAR(2)       NOT NULL,
    region          VARCHAR(20)   NOT NULL,
    capacity_units  INT           NOT NULL CONSTRAINT CK_wh_capacity CHECK (capacity_units > 0),
    manager_name    NVARCHAR(80)  NULL,
    opened_date     DATE          NULL,
    bronze_batch_id VARCHAR(30)   NULL,
    CONSTRAINT PK_dim_warehouse PRIMARY KEY (warehouse_id)
);

CREATE TABLE logistics.dim_partner (
    partner_id           VARCHAR(10)   NOT NULL,
    partner_name         NVARCHAR(100) NOT NULL,
    transport_mode       VARCHAR(10)   NOT NULL CONSTRAINT CK_partner_mode CHECK (transport_mode IN ('Road','Rail','Air')),
    contact_email        NVARCHAR(200) NULL,
    contract_rate_per_km DECIMAL(8,2)  NOT NULL CONSTRAINT CK_partner_rate CHECK (contract_rate_per_km > 0),
    sla_days             TINYINT       NOT NULL,
    contract_start_date  DATE          NULL,
    is_active            BIT           NOT NULL,
    bronze_batch_id      VARCHAR(30)   NULL,
    CONSTRAINT PK_dim_partner PRIMARY KEY (partner_id)
);
GO

/* ================================= FACTS ================================== */
CREATE TABLE logistics.fact_shipment (
    shipment_id             VARCHAR(12)   NOT NULL,
    customer_id             VARCHAR(10)   NOT NULL,
    origin_warehouse_id     VARCHAR(10)   NOT NULL,
    destination_city        NVARCHAR(60)  NOT NULL,
    destination_state       CHAR(2)       NOT NULL,
    partner_id              VARCHAR(10)   NOT NULL,
    route_id                VARCHAR(15)   NOT NULL,
    ship_date               DATE          NOT NULL,
    promised_delivery_date  DATE          NOT NULL,
    actual_delivery_date    DATE          NULL,        -- NULL while In Transit or Cancelled
    shipment_status         VARCHAR(12)   NOT NULL,
    weight_kg               DECIMAL(10,1) NULL,
    distance_km             DECIMAL(10,1) NOT NULL,
    shipping_cost           DECIMAL(12,2) NOT NULL,
    priority                VARCHAR(10)   NULL,
    weight_imputed          BIT           NULL,
    cost_imputed            BIT           NULL,
    status_corrected        BIT           NULL,
    planned_transit_days    INT           NULL,
    delivery_days           INT           NULL,
    delay_days              INT           NULL,
    is_on_time              BIT           NULL,
    ship_month              CHAR(7)       NOT NULL,    -- 'YYYY-MM'
    cost_per_km             DECIMAL(10,4) NULL,
    bronze_batch_id         VARCHAR(30)   NULL,
    CONSTRAINT PK_fact_shipment PRIMARY KEY (shipment_id),
    CONSTRAINT FK_ship_customer  FOREIGN KEY (customer_id)         REFERENCES logistics.dim_customer (customer_id),
    CONSTRAINT FK_ship_warehouse FOREIGN KEY (origin_warehouse_id) REFERENCES logistics.dim_warehouse (warehouse_id),
    CONSTRAINT FK_ship_partner   FOREIGN KEY (partner_id)          REFERENCES logistics.dim_partner (partner_id),
    CONSTRAINT CK_ship_status    CHECK (shipment_status IN ('Delivered','Delayed','In Transit','Cancelled')),
    CONSTRAINT CK_ship_cost      CHECK (shipping_cost >= 0),
    CONSTRAINT CK_ship_dates     CHECK (actual_delivery_date IS NULL OR actual_delivery_date >= ship_date)
);

CREATE TABLE logistics.fact_tracking_event (
    event_id         VARCHAR(12)   NOT NULL,
    shipment_id      VARCHAR(12)   NOT NULL,
    event_type       VARCHAR(20)   NOT NULL,
    event_timestamp  DATETIME2(0)  NOT NULL,
    event_location   NVARCHAR(80)  NULL,
    warehouse_id     VARCHAR(10)   NULL,
    remarks          NVARCHAR(200) NULL,
    bronze_batch_id  VARCHAR(30)   NULL,
    CONSTRAINT PK_fact_tracking_event PRIMARY KEY (event_id),
    CONSTRAINT FK_event_shipment FOREIGN KEY (shipment_id) REFERENCES logistics.fact_shipment (shipment_id),
    CONSTRAINT CK_event_type CHECK (event_type IN ('ORDER_PICKED_UP','ARRIVED_AT_HUB','DEPARTED_HUB',
                                                   'OUT_FOR_DELIVERY','DELIVERED','DELAY_EXCEPTION','CANCELLED'))
);

CREATE TABLE logistics.fact_inventory_snapshot (
    inventory_id      VARCHAR(12)   NOT NULL,
    warehouse_id      VARCHAR(10)   NOT NULL,
    sku               VARCHAR(10)   NOT NULL,
    product_name      NVARCHAR(80)  NOT NULL,
    category          NVARCHAR(40)  NOT NULL,
    snapshot_date     DATE          NOT NULL,
    quantity_on_hand  INT           NOT NULL,
    reorder_level     INT           NOT NULL,
    unit_cost         DECIMAL(10,2) NOT NULL,
    bronze_batch_id   VARCHAR(30)   NULL,
    CONSTRAINT PK_fact_inventory PRIMARY KEY (inventory_id),
    CONSTRAINT FK_inv_warehouse FOREIGN KEY (warehouse_id) REFERENCES logistics.dim_warehouse (warehouse_id),
    CONSTRAINT UQ_inv_natural   UNIQUE (warehouse_id, sku, snapshot_date),
    CONSTRAINT CK_inv_qty       CHECK (quantity_on_hand >= 0)
);
GO

/* =============================== GOLD TABLES ============================== */
CREATE TABLE logistics.gold_kpi_summary (
    business_question_no INT          NOT NULL,
    metric               VARCHAR(80)  NOT NULL,
    value                DECIMAL(18,2) NULL,
    unit                 VARCHAR(20)  NULL,
    description          NVARCHAR(300) NULL,
    CONSTRAINT PK_gold_kpi PRIMARY KEY (metric)
);

CREATE TABLE logistics.gold_shipment_performance_summary (
    ship_month            CHAR(7)       NOT NULL,
    total_shipments       INT           NOT NULL,
    delivered_shipments   INT           NOT NULL,
    delayed_shipments     INT           NOT NULL,
    in_transit_shipments  INT           NOT NULL,
    cancelled_shipments   INT           NOT NULL,
    on_time_delivery_pct  DECIMAL(6,2)  NULL,
    delay_rate_pct        DECIMAL(6,2)  NULL,
    avg_delivery_days     DECIMAL(6,2)  NULL,
    CONSTRAINT PK_gold_perf PRIMARY KEY (ship_month)
);

CREATE TABLE logistics.gold_delivery_delay_analysis (
    delay_reason          NVARCHAR(60)  NOT NULL,
    delay_bucket          VARCHAR(12)   NOT NULL,
    delayed_shipments     INT           NOT NULL,
    avg_delay_days        DECIMAL(6,2)  NULL,
    total_delay_days      INT           NULL,
    total_shipping_cost   DECIMAL(18,2) NULL,
    pct_of_all_delays     DECIMAL(6,2)  NULL,
    CONSTRAINT PK_gold_delay PRIMARY KEY (delay_reason, delay_bucket)
);

CREATE TABLE logistics.gold_customer_delay_analysis (
    customer_id          VARCHAR(10)   NOT NULL,
    customer_name        NVARCHAR(120) NOT NULL,
    segment              VARCHAR(30)   NULL,
    total_shipments      INT           NOT NULL,
    completed_shipments  INT           NOT NULL,
    delayed_shipments    INT           NOT NULL,
    avg_delay_days       DECIMAL(6,2)  NULL,
    delay_rate_pct       DECIMAL(6,2)  NULL,
    delay_rank           INT           NOT NULL,
    CONSTRAINT PK_gold_cust_delay PRIMARY KEY (customer_id)
);

CREATE TABLE logistics.gold_warehouse_inventory_summary (
    warehouse_id              VARCHAR(10)   NOT NULL,
    warehouse_name            NVARCHAR(100) NOT NULL,
    city                      NVARCHAR(60)  NULL,
    region                    VARCHAR(20)   NULL,
    capacity_units            INT           NULL,
    snapshot_date             DATE          NULL,
    total_skus                INT           NOT NULL,
    total_units               INT           NOT NULL,
    inventory_value           DECIMAL(18,2) NULL,
    low_stock_skus            INT           NOT NULL,
    critical_skus             INT           NOT NULL,
    overstock_skus            INT           NOT NULL,
    low_stock_pct             DECIMAL(6,2)  NULL,
    capacity_utilization_pct  DECIMAL(6,2)  NULL,
    stock_status              VARCHAR(25)   NOT NULL,
    low_stock_rank            INT           NOT NULL,
    CONSTRAINT PK_gold_wh_inv PRIMARY KEY (warehouse_id)
);

CREATE TABLE logistics.gold_partner_performance (
    partner_id            VARCHAR(10)   NOT NULL,
    partner_name          NVARCHAR(100) NOT NULL,
    transport_mode        VARCHAR(10)   NULL,
    total_shipments       INT           NOT NULL,
    delivered_on_time     INT           NOT NULL,
    delayed_shipments     INT           NOT NULL,
    in_transit_shipments  INT           NOT NULL,
    cancelled_shipments   INT           NOT NULL,
    on_time_pct           DECIMAL(6,2)  NULL,
    delay_rate_pct        DECIMAL(6,2)  NULL,
    avg_delivery_days     DECIMAL(6,2)  NULL,
    avg_delay_days        DECIMAL(6,2)  NULL,
    sla_breach_pct        DECIMAL(6,2)  NULL,
    total_shipping_cost   DECIMAL(18,2) NULL,
    avg_cost_per_shipment DECIMAL(12,2) NULL,
    avg_cost_per_km       DECIMAL(10,2) NULL,
    delay_rank            INT           NOT NULL,
    CONSTRAINT PK_gold_partner PRIMARY KEY (partner_id)
);

CREATE TABLE logistics.gold_monthly_cost_analysis (
    ship_month             CHAR(7)       NOT NULL,
    shipments              INT           NOT NULL,
    total_cost             DECIMAL(18,2) NOT NULL,
    avg_cost_per_shipment  DECIMAL(12,2) NULL,
    total_distance_km      DECIMAL(18,2) NULL,
    road_cost              DECIMAL(18,2) NULL,
    rail_cost              DECIMAL(18,2) NULL,
    air_cost               DECIMAL(18,2) NULL,
    cost_per_km            DECIMAL(10,4) NULL,
    mom_change_pct         DECIMAL(8,2)  NULL,           -- NULL for the first month
    cumulative_cost        DECIMAL(18,2) NULL,
    CONSTRAINT PK_gold_cost PRIMARY KEY (ship_month)
);

CREATE TABLE logistics.gold_route_performance (
    route_id              VARCHAR(15)   NOT NULL,
    origin_warehouse_id   VARCHAR(10)   NOT NULL,
    origin_city           NVARCHAR(60)  NULL,
    destination_city      NVARCHAR(60)  NULL,
    total_shipments       INT           NOT NULL,
    delivered_on_time     INT           NOT NULL,
    delayed_shipments     INT           NOT NULL,
    in_transit_shipments  INT           NOT NULL,
    cancelled_shipments   INT           NOT NULL,
    delay_rate_pct        DECIMAL(6,2)  NULL,
    avg_delivery_days     DECIMAL(6,2)  NULL,
    avg_delay_days        DECIMAL(6,2)  NULL,
    distance_km           DECIMAL(10,2) NULL,
    total_shipping_cost   DECIMAL(18,2) NULL,
    delay_rank            INT           NOT NULL,
    CONSTRAINT PK_gold_route PRIMARY KEY (route_id)
);

/* ============================== OPERATIONS LOG ============================= */
CREATE TABLE logistics.pipeline_run_log (
    log_id         BIGINT IDENTITY(1,1) NOT NULL,
    pipeline_name  VARCHAR(100)  NOT NULL,
    run_id         VARCHAR(60)   NOT NULL,
    run_date       DATE          NULL,
    status         VARCHAR(20)   NOT NULL,            -- Started / Succeeded / Failed
    message        NVARCHAR(2000) NULL,
    logged_at_utc  DATETIME2(0)  NOT NULL CONSTRAINT DF_runlog_ts DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_pipeline_run_log PRIMARY KEY (log_id)
);
GO

/* ================================= INDEXES ================================= */
CREATE INDEX IX_ship_ship_date  ON logistics.fact_shipment (ship_date) INCLUDE (shipment_status, shipping_cost, delivery_days);
CREATE INDEX IX_ship_partner    ON logistics.fact_shipment (partner_id, shipment_status);
CREATE INDEX IX_ship_customer   ON logistics.fact_shipment (customer_id, shipment_status);
CREATE INDEX IX_ship_route      ON logistics.fact_shipment (route_id) INCLUDE (shipment_status, delay_days);
CREATE INDEX IX_event_shipment  ON logistics.fact_tracking_event (shipment_id, event_timestamp);
CREATE INDEX IX_inv_wh_snapshot ON logistics.fact_inventory_snapshot (warehouse_id, snapshot_date) INCLUDE (sku, quantity_on_hand, reorder_level);
GO

/* ================================ REPORT VIEW ============================== */
CREATE VIEW logistics.vw_shipment_detail
AS
SELECT  s.shipment_id, s.ship_month, s.ship_date, s.promised_delivery_date, s.actual_delivery_date,
        s.shipment_status, s.delivery_days, s.delay_days, s.shipping_cost, s.distance_km, s.weight_kg,
        s.priority, s.route_id,
        c.customer_id, c.customer_name, c.segment,
        w.warehouse_id, w.warehouse_name, w.city AS origin_city, w.region AS origin_region,
        s.destination_city,
        p.partner_id, p.partner_name, p.transport_mode, p.sla_days
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_customer  AS c ON c.customer_id  = s.customer_id
JOIN    logistics.dim_warehouse AS w ON w.warehouse_id = s.origin_warehouse_id
JOIN    logistics.dim_partner   AS p ON p.partner_id   = s.partner_id;
GO
