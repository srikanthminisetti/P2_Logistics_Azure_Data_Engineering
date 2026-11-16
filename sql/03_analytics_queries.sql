/* =====================================================================================
   03_analytics_queries.sql  -  24 analytics queries for Azure SQL Database (T-SQL)
   Q01-Q10 answer the 10 business questions; Q11-Q24 demonstrate JOINs, GROUP BY/HAVING,
   subqueries, CTEs, window functions, ranking, CASE and date functions.
   Prerequisite: tables from 02_table_creation.sql loaded (ADF PL_Load_Gold_To_SQL).
   Status: written for Azure SQL (T-SQL); not executed against Azure in this delivery.
   ===================================================================================== */

/* ---------- Q01  Total number of shipments (business question 1) ---------- */
SELECT COUNT(*) AS total_shipments
FROM   logistics.fact_shipment;

/* ---------- Q02  Shipments by status with % share (BQ2) : GROUP BY + window ---------- */
SELECT  shipment_status,
        COUNT(*)                                                    AS shipments,
        CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct_of_total
FROM    logistics.fact_shipment
GROUP BY shipment_status
ORDER BY shipments DESC;

/* ---------- Q03  Partners with the highest delivery delays (BQ3) : JOIN, HAVING, RANK ---------- */
SELECT  RANK() OVER (ORDER BY 1.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)
                                 / NULLIF(SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) DESC) AS delay_rank,
        p.partner_id, p.partner_name, p.transport_mode,
        SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END) AS completed_shipments,
        SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)                AS delayed_shipments,
        CAST(100.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS DECIMAL(5,2)) AS delay_rate_pct
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
GROUP BY p.partner_id, p.partner_name, p.transport_mode
HAVING  SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END) >= 100
ORDER BY delay_rate_pct DESC;

/* ---------- Q04  Average delivery time (BQ4) : overall and by transport mode ---------- */
SELECT  ISNULL(p.transport_mode, 'ALL MODES') AS transport_mode,
        CAST(AVG(CAST(s.delivery_days AS DECIMAL(6,2))) AS DECIMAL(5,2)) AS avg_delivery_days,
        COUNT(*) AS delivered_shipments
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
WHERE   s.shipment_status IN ('Delivered','Delayed')
GROUP BY ROLLUP (p.transport_mode);

/* ---------- Q05  Warehouses with low inventory (BQ5) : CTE on the latest snapshot ---------- */
WITH latest AS (
    SELECT warehouse_id, MAX(snapshot_date) AS snapshot_date
    FROM   logistics.fact_inventory_snapshot
    GROUP BY warehouse_id
)
SELECT  w.warehouse_id, w.city,
        COUNT(*)                                                         AS skus,
        SUM(CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 1 ELSE 0 END) AS low_stock_skus,
        CAST(100.0 * SUM(CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS low_stock_pct
FROM    logistics.fact_inventory_snapshot AS i
JOIN    latest                            AS l ON l.warehouse_id = i.warehouse_id AND l.snapshot_date = i.snapshot_date
JOIN    logistics.dim_warehouse           AS w ON w.warehouse_id = i.warehouse_id
GROUP BY w.warehouse_id, w.city
HAVING  100.0 * SUM(CASE WHEN i.quantity_on_hand <= i.reorder_level THEN 1 ELSE 0 END) / COUNT(*) >= 25
ORDER BY low_stock_pct DESC;

/* ---------- Q06  On-time delivery percentage (BQ6) ---------- */
SELECT  CAST(100.0 * SUM(CASE WHEN shipment_status = 'Delivered' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS DECIMAL(5,2)) AS on_time_delivery_pct
FROM    logistics.fact_shipment;

/* ---------- Q07  Routes with the most delays (BQ7) : TOP + ORDER BY ---------- */
SELECT TOP (10)
        s.route_id, w.city AS origin_city, s.destination_city,
        COUNT(*) AS shipments,
        SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_shipments,
        CAST(100.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS DECIMAL(5,2)) AS delay_rate_pct
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_warehouse AS w ON w.warehouse_id = s.origin_warehouse_id
GROUP BY s.route_id, w.city, s.destination_city
ORDER BY delayed_shipments DESC, delay_rate_pct DESC;

/* ---------- Q08  Monthly transportation cost (BQ8) : date functions + LAG for month-over-month ---------- */
WITH monthly AS (
    SELECT  DATEFROMPARTS(YEAR(ship_date), MONTH(ship_date), 1) AS month_start,
            COUNT(*)           AS shipments,
            SUM(shipping_cost) AS total_cost
    FROM    logistics.fact_shipment
    GROUP BY DATEFROMPARTS(YEAR(ship_date), MONTH(ship_date), 1)
)
SELECT  month_start, shipments, total_cost,
        CAST(100.0 * (total_cost - LAG(total_cost) OVER (ORDER BY month_start))
             / NULLIF(LAG(total_cost) OVER (ORDER BY month_start), 0) AS DECIMAL(7,2)) AS mom_change_pct,
        SUM(total_cost) OVER (ORDER BY month_start ROWS UNBOUNDED PRECEDING)           AS cumulative_cost
FROM    monthly
ORDER BY month_start;

/* ---------- Q09  Customers with the most delayed deliveries (BQ9) : DENSE_RANK ---------- */
SELECT TOP (10)
        DENSE_RANK() OVER (ORDER BY SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END) DESC) AS delay_rank,
        c.customer_id, c.customer_name, c.segment,
        SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_shipments,
        COUNT(*) AS total_shipments
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_customer  AS c ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name, c.segment
ORDER BY delayed_shipments DESC;

/* ---------- Q10  Inventory levels across warehouses (BQ10) : latest snapshot, value and units ---------- */
SELECT  w.warehouse_id, w.city, w.capacity_units,
        SUM(i.quantity_on_hand)                                   AS units_on_hand,
        CAST(SUM(1.0 * i.quantity_on_hand * i.unit_cost) AS DECIMAL(18,2)) AS inventory_value,
        CAST(100.0 * SUM(i.quantity_on_hand) / w.capacity_units AS DECIMAL(5,2)) AS capacity_utilization_pct
FROM    logistics.fact_inventory_snapshot AS i
JOIN    logistics.dim_warehouse           AS w ON w.warehouse_id = i.warehouse_id
WHERE   i.snapshot_date = (SELECT MAX(snapshot_date) FROM logistics.fact_inventory_snapshot)
GROUP BY w.warehouse_id, w.city, w.capacity_units
ORDER BY units_on_hand DESC;

/* ---------- Q11  Correlated subquery: shipments costing more than 2x their partner's average ---------- */
SELECT TOP (20) s.shipment_id, s.partner_id, s.shipping_cost,
       (SELECT CAST(AVG(s2.shipping_cost) AS DECIMAL(12,2)) FROM logistics.fact_shipment AS s2 WHERE s2.partner_id = s.partner_id) AS partner_avg_cost
FROM   logistics.fact_shipment AS s
WHERE  s.shipping_cost > 2 * (SELECT AVG(s2.shipping_cost) FROM logistics.fact_shipment AS s2 WHERE s2.partner_id = s.partner_id)
ORDER BY s.shipping_cost DESC;

/* ---------- Q12  NOT EXISTS: customers that never had a delayed delivery (despite 10+ shipments) ---------- */
SELECT c.customer_id, c.customer_name, x.shipments
FROM   logistics.dim_customer AS c
CROSS APPLY (SELECT COUNT(*) AS shipments FROM logistics.fact_shipment AS s WHERE s.customer_id = c.customer_id) AS x
WHERE  x.shipments >= 10
  AND  NOT EXISTS (SELECT 1 FROM logistics.fact_shipment AS d WHERE d.customer_id = c.customer_id AND d.shipment_status = 'Delayed')
ORDER BY x.shipments DESC;

/* ---------- Q13  CTEs + window frame: 3-month moving average of each partner's delay rate ---------- */
WITH monthly AS (
    SELECT  partner_id, ship_month,
            100.0 * SUM(CASE WHEN shipment_status = 'Delayed' THEN 1 ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS delay_rate
    FROM    logistics.fact_shipment
    GROUP BY partner_id, ship_month
)
SELECT  partner_id, ship_month,
        CAST(delay_rate AS DECIMAL(5,2)) AS delay_rate_pct,
        CAST(AVG(delay_rate) OVER (PARTITION BY partner_id ORDER BY ship_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS DECIMAL(5,2)) AS moving_avg_3m
FROM    monthly
ORDER BY partner_id, ship_month;

/* ---------- Q14  ROW_NUMBER per group: top 3 most delayed routes for every origin warehouse ---------- */
WITH route_delays AS (
    SELECT origin_warehouse_id, route_id, destination_city,
           SUM(CASE WHEN shipment_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_shipments
    FROM   logistics.fact_shipment
    GROUP BY origin_warehouse_id, route_id, destination_city
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY origin_warehouse_id ORDER BY delayed_shipments DESC, route_id) AS rn
    FROM   route_delays
)
SELECT origin_warehouse_id, rn AS rank_in_warehouse, route_id, destination_city, delayed_shipments
FROM   ranked
WHERE  rn <= 3
ORDER BY origin_warehouse_id, rn;

/* ---------- Q15  CASE: delay severity buckets ---------- */
SELECT  CASE WHEN delay_days = 0 THEN 'On time'
             WHEN delay_days = 1 THEN '1 day late'
             WHEN delay_days BETWEEN 2 AND 3 THEN '2-3 days late'
             ELSE '4+ days late' END AS delay_severity,
        COUNT(*) AS shipments,
        CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) AS pct
FROM    logistics.fact_shipment
WHERE   shipment_status IN ('Delivered','Delayed')
GROUP BY CASE WHEN delay_days = 0 THEN 'On time'
              WHEN delay_days = 1 THEN '1 day late'
              WHEN delay_days BETWEEN 2 AND 3 THEN '2-3 days late'
              ELSE '4+ days late' END
ORDER BY MIN(delay_days);

/* ---------- Q16  Date functions: delay rate by quarter and weekday of dispatch ---------- */
SELECT  YEAR(ship_date) AS yr, DATEPART(QUARTER, ship_date) AS qtr, DATENAME(WEEKDAY, ship_date) AS dispatch_weekday,
        COUNT(*) AS shipments,
        CAST(100.0 * SUM(CASE WHEN shipment_status = 'Delayed' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS DECIMAL(5,2)) AS delay_rate_pct
FROM    logistics.fact_shipment
GROUP BY YEAR(ship_date), DATEPART(QUARTER, ship_date), DATENAME(WEEKDAY, ship_date), DATEPART(WEEKDAY, ship_date)
ORDER BY yr, qtr, DATEPART(WEEKDAY, ship_date);

/* ---------- Q17  Transportation cost analysis: cost per km and per kg by mode and priority ---------- */
SELECT  p.transport_mode, s.priority,
        COUNT(*)                                                       AS shipments,
        CAST(SUM(s.shipping_cost) AS DECIMAL(18,2))                    AS total_cost,
        CAST(SUM(s.shipping_cost) / NULLIF(SUM(s.distance_km), 0) AS DECIMAL(8,3)) AS cost_per_km,
        CAST(SUM(s.shipping_cost) / NULLIF(SUM(s.weight_kg), 0) AS DECIMAL(8,3))   AS cost_per_kg
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
WHERE   s.shipment_status <> 'Cancelled'
GROUP BY p.transport_mode, s.priority
ORDER BY p.transport_mode, cost_per_km DESC;

/* ---------- Q18  SLA breach: delivery took longer than the partner's contractual SLA days ---------- */
SELECT  p.partner_name, p.sla_days,
        COUNT(*) AS completed,
        SUM(CASE WHEN s.delivery_days > p.sla_days THEN 1 ELSE 0 END) AS sla_breaches,
        CAST(100.0 * SUM(CASE WHEN s.delivery_days > p.sla_days THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS sla_breach_pct
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
WHERE   s.delivery_days IS NOT NULL
GROUP BY p.partner_name, p.sla_days
ORDER BY sla_breach_pct DESC;

/* ---------- Q19  Latest tracking event per shipment (ROW_NUMBER) for shipments still in transit ---------- */
WITH last_event AS (
    SELECT  e.shipment_id, e.event_type, e.event_location, e.event_timestamp,
            ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_timestamp DESC) AS rn
    FROM    logistics.fact_tracking_event AS e
)
SELECT  s.shipment_id, s.ship_date, s.promised_delivery_date, le.event_type AS last_event, le.event_location, le.event_timestamp,
        DATEDIFF(DAY, s.promised_delivery_date, CAST('2026-06-30' AS DATE)) AS days_past_promise_at_snapshot
FROM    logistics.fact_shipment AS s
JOIN    last_event              AS le ON le.shipment_id = s.shipment_id AND le.rn = 1
WHERE   s.shipment_status = 'In Transit'
ORDER BY s.ship_date;

/* ---------- Q20  Root causes: delay reasons from DELAY_EXCEPTION events ---------- */
SELECT  e.remarks AS delay_reason, COUNT(DISTINCT e.shipment_id) AS delayed_shipments,
        CAST(AVG(CAST(s.delay_days AS DECIMAL(5,2))) AS DECIMAL(5,2)) AS avg_delay_days
FROM    logistics.fact_tracking_event AS e
JOIN    logistics.fact_shipment       AS s ON s.shipment_id = e.shipment_id
WHERE   e.event_type = 'DELAY_EXCEPTION'
GROUP BY e.remarks
ORDER BY delayed_shipments DESC;

/* ---------- Q21  Inventory trend: month-over-month change with LAG, SKUs falling 3 months in a row ---------- */
WITH trend AS (
    SELECT  warehouse_id, sku, snapshot_date, quantity_on_hand,
            LAG(quantity_on_hand, 1) OVER (PARTITION BY warehouse_id, sku ORDER BY snapshot_date) AS prev_1,
            LAG(quantity_on_hand, 2) OVER (PARTITION BY warehouse_id, sku ORDER BY snapshot_date) AS prev_2
    FROM    logistics.fact_inventory_snapshot
)
SELECT  t.warehouse_id, t.sku, t.snapshot_date, t.prev_2, t.prev_1, t.quantity_on_hand
FROM    trend AS t
WHERE   t.snapshot_date = (SELECT MAX(snapshot_date) FROM logistics.fact_inventory_snapshot)
  AND   t.quantity_on_hand < t.prev_1 AND t.prev_1 < t.prev_2
ORDER BY t.warehouse_id, t.sku;

/* ---------- Q22  Replenishment recommendation: order enough to reach 2x reorder level ---------- */
SELECT  i.warehouse_id, i.sku, i.product_name, i.quantity_on_hand, i.reorder_level,
        CASE WHEN i.quantity_on_hand <= i.reorder_level * 0.5 THEN 'Critical' ELSE 'Low' END AS stock_status,
        i.reorder_level * 2 - i.quantity_on_hand                       AS recommended_order_qty,
        CAST((i.reorder_level * 2 - i.quantity_on_hand) * i.unit_cost AS DECIMAL(12,2)) AS estimated_order_value
FROM    logistics.fact_inventory_snapshot AS i
WHERE   i.snapshot_date = (SELECT MAX(snapshot_date) FROM logistics.fact_inventory_snapshot)
  AND   i.quantity_on_hand <= i.reorder_level
ORDER BY estimated_order_value DESC;

/* ---------- Q23  Does low stock hurt delivery speed?  join shipments to the Gold warehouse status ---------- */
SELECT  g.stock_status                                                        AS origin_warehouse_stock_status,
        COUNT(*)                                                              AS shipments,
        CAST(AVG(CAST(s.delivery_days AS DECIMAL(6,2))) AS DECIMAL(5,2))      AS avg_delivery_days,
        CAST(100.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0) AS DECIMAL(5,2))                           AS delay_rate_pct
FROM    logistics.fact_shipment                 AS s
JOIN    logistics.gold_warehouse_inventory_summary AS g ON g.warehouse_id = s.origin_warehouse_id
WHERE   s.shipment_status IN ('Delivered','Delayed')
GROUP BY g.stock_status
ORDER BY delay_rate_pct DESC;

/* ---------- Q24  PERCENTILE_CONT: median and 90th-percentile delivery days per transport mode ---------- */
SELECT DISTINCT
        p.transport_mode,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.delivery_days) OVER (PARTITION BY p.transport_mode) AS median_delivery_days,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY s.delivery_days) OVER (PARTITION BY p.transport_mode) AS p90_delivery_days
FROM    logistics.fact_shipment AS s
JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
WHERE   s.delivery_days IS NOT NULL;
