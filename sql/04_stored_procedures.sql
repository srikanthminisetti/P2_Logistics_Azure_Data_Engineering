/* =====================================================================================
   04_stored_procedures.sql  -  procedures used by ADF and by analysts (Azure SQL / T-SQL)
   usp_log_pipeline_run        called by ADF (Stored Procedure activity) to write the run log
   usp_prepare_reload          called by ADF before loading: clears data child-first (FK safe)
   usp_get_partner_performance parameterised partner KPI report for a date range
   usp_low_stock_alert         SKUs at/below reorder level with recommended order quantity
   usp_refresh_monthly_cost    rebuilds gold_monthly_cost_analysis directly from fact_shipment
   ===================================================================================== */

CREATE OR ALTER PROCEDURE logistics.usp_log_pipeline_run
    @pipeline_name VARCHAR(100),
    @run_id        VARCHAR(60),
    @run_date      VARCHAR(10) = NULL,
    @status        VARCHAR(20),
    @message       NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO logistics.pipeline_run_log (pipeline_name, run_id, run_date, status, message)
    VALUES (@pipeline_name, @run_id, TRY_CONVERT(DATE, @run_date), @status, LEFT(@message, 2000));
END;
GO

CREATE OR ALTER PROCEDURE logistics.usp_prepare_reload
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        -- children first, parents last (TRUNCATE is not allowed on FK-referenced tables)
        DELETE FROM logistics.fact_tracking_event;
        DELETE FROM logistics.fact_inventory_snapshot;
        DELETE FROM logistics.fact_shipment;
        DELETE FROM logistics.dim_customer;
        DELETE FROM logistics.dim_warehouse;
        DELETE FROM logistics.dim_partner;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;   -- let ADF see the failure and apply its retry policy
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE logistics.usp_get_partner_performance
    @start_date      DATE,
    @end_date        DATE,
    @min_shipments   INT = 50
AS
BEGIN
    SET NOCOUNT ON;
    IF @start_date > @end_date
        THROW 50001, 'start_date must be on or before end_date', 1;

    SELECT  p.partner_id, p.partner_name, p.transport_mode,
            COUNT(*) AS shipments,
            SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END) AS delayed_shipments,
            CAST(100.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END)
                 / NULLIF(SUM(CASE WHEN s.shipment_status IN ('Delivered','Delayed') THEN 1 ELSE 0 END), 0) AS DECIMAL(5,2)) AS delay_rate_pct,
            CAST(AVG(CAST(s.delivery_days AS DECIMAL(6,2))) AS DECIMAL(5,2)) AS avg_delivery_days,
            CAST(SUM(s.shipping_cost) AS DECIMAL(18,2)) AS total_cost,
            RANK() OVER (ORDER BY 1.0 * SUM(CASE WHEN s.shipment_status = 'Delayed' THEN 1 ELSE 0 END) / COUNT(*) DESC) AS delay_rank
    FROM    logistics.fact_shipment AS s
    JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
    WHERE   s.ship_date BETWEEN @start_date AND @end_date
    GROUP BY p.partner_id, p.partner_name, p.transport_mode
    HAVING  COUNT(*) >= @min_shipments
    ORDER BY delay_rank;
END;
GO

CREATE OR ALTER PROCEDURE logistics.usp_low_stock_alert
    @warehouse_id  VARCHAR(10) = NULL,     -- NULL = all warehouses
    @snapshot_date DATE        = NULL      -- NULL = latest snapshot
AS
BEGIN
    SET NOCOUNT ON;
    IF @snapshot_date IS NULL
        SELECT @snapshot_date = MAX(snapshot_date) FROM logistics.fact_inventory_snapshot;

    SELECT  i.warehouse_id, w.city, i.sku, i.product_name, i.category,
            i.quantity_on_hand, i.reorder_level,
            CASE WHEN i.quantity_on_hand <= i.reorder_level * 0.5 THEN 'Critical' ELSE 'Low' END AS stock_status,
            i.reorder_level * 2 - i.quantity_on_hand AS recommended_order_qty
    FROM    logistics.fact_inventory_snapshot AS i
    JOIN    logistics.dim_warehouse           AS w ON w.warehouse_id = i.warehouse_id
    WHERE   i.snapshot_date = @snapshot_date
      AND   i.quantity_on_hand <= i.reorder_level
      AND   (@warehouse_id IS NULL OR i.warehouse_id = @warehouse_id)
    ORDER BY stock_status, recommended_order_qty DESC;
END;
GO

CREATE OR ALTER PROCEDURE logistics.usp_refresh_monthly_cost
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        TRUNCATE TABLE logistics.gold_monthly_cost_analysis;

        WITH monthly AS (
            SELECT  s.ship_month,
                    COUNT(*)                                                            AS shipments,
                    SUM(s.shipping_cost)                                                AS total_cost,
                    SUM(s.distance_km)                                                  AS total_distance_km,
                    SUM(CASE WHEN p.transport_mode = 'Road' THEN s.shipping_cost ELSE 0 END) AS road_cost,
                    SUM(CASE WHEN p.transport_mode = 'Rail' THEN s.shipping_cost ELSE 0 END) AS rail_cost,
                    SUM(CASE WHEN p.transport_mode = 'Air'  THEN s.shipping_cost ELSE 0 END) AS air_cost
            FROM    logistics.fact_shipment AS s
            JOIN    logistics.dim_partner   AS p ON p.partner_id = s.partner_id
            GROUP BY s.ship_month
        )
        INSERT INTO logistics.gold_monthly_cost_analysis
               (ship_month, shipments, total_cost, avg_cost_per_shipment, total_distance_km,
                road_cost, rail_cost, air_cost, cost_per_km, mom_change_pct, cumulative_cost)
        SELECT  ship_month, shipments, total_cost, total_cost / shipments, total_distance_km,
                road_cost, rail_cost, air_cost, total_cost / NULLIF(total_distance_km, 0),
                100.0 * (total_cost - LAG(total_cost) OVER (ORDER BY ship_month))
                      / NULLIF(LAG(total_cost) OVER (ORDER BY ship_month), 0),
                SUM(total_cost) OVER (ORDER BY ship_month ROWS UNBOUNDED PRECEDING)
        FROM    monthly;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

/* Example calls
EXEC logistics.usp_get_partner_performance @start_date = '2025-07-01', @end_date = '2026-06-30';
EXEC logistics.usp_low_stock_alert @warehouse_id = 'WH05';
EXEC logistics.usp_refresh_monthly_cost;
*/
