"""Unit tests for the Silver / enrichment / Gold business rules (run with:  pytest -v)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.ingestion.bronze_ingestion import add_audit_columns
from src.transformations import gold_analytics as ga
from src.transformations.inventory_transformation import stock_status
from src.transformations.shipment_transformation import delay_bucket
from src.transformations.silver_cleaning import clean_inventory, clean_shipments, clean_tracking_events

SHIP_COLS = ["shipment_id", "customer_id", "origin_warehouse_id", "destination_city", "destination_state", "partner_id",
             "route_id", "ship_date", "promised_delivery_date", "actual_delivery_date", "shipment_status", "weight_kg",
             "distance_km", "shipping_cost", "priority"]


def ship_row(**kw):
    base = dict(shipment_id="SHP1", customer_id="C1", origin_warehouse_id="WH01", destination_city="Denver", destination_state="CO",
                partner_id="TP01", route_id="WH01-DEN", ship_date="2026-01-05", promised_delivery_date="2026-01-08",
                actual_delivery_date="2026-01-08", shipment_status="Delivered", weight_kg="40", distance_km="1000",
                shipping_cost="900", priority="Standard")
    base.update(kw)
    return base


def bronze(rows, cols, table="shipments"):
    df = pd.DataFrame(rows, columns=cols).astype("string")
    return add_audit_columns(df, table, "TESTBATCH")


CUST = pd.DataFrame({"customer_id": ["C1", "C2"]})
WH = pd.DataFrame({"warehouse_id": ["WH01", "WH02"]})
PAR = pd.DataFrame({"partner_id": ["TP01"], "contract_rate_per_km": [0.9]})


def run_ship(rows):
    return clean_shipments(bronze(rows, SHIP_COLS), CUST, WH, PAR)


def reasons(rej):
    return dict(zip(rej["record_key"], rej["reject_reason"]))


# ------------------------------------------------------------------ shipments
def test_status_is_derived_from_dates_not_trusted():
    clean, _ = run_ship([ship_row(shipment_id="A", actual_delivery_date="2026-01-10", shipment_status="Delivered"),   # late but labelled Delivered
                         ship_row(shipment_id="B", actual_delivery_date="2026-01-07", shipment_status="DELAYED")])    # early but labelled Delayed
    st = clean.set_index("shipment_id")
    assert st.loc["A", "shipment_status"] == "Delayed" and bool(st.loc["A", "status_corrected"])
    assert st.loc["B", "shipment_status"] == "Delivered"
    assert st.loc["A", "delay_days"] == 2 and st.loc["B", "delay_days"] == 0


def test_rejection_rules_one_reason_each():
    rows = [ship_row(shipment_id="OK"),
            ship_row(shipment_id="OK"),                                                    # duplicate
            ship_row(shipment_id="BADSTAT", shipment_status="UNKNOWN"),
            ship_row(shipment_id="NOWH", origin_warehouse_id=None),
            ship_row(shipment_id="ORPH", origin_warehouse_id="WH99"),
            ship_row(shipment_id="ORPHC", customer_id="C9999"),
            ship_row(shipment_id="BEFORE", actual_delivery_date="2026-01-01"),
            ship_row(shipment_id="BADDATE", ship_date="2026-13-45"),
            ship_row(shipment_id="NODELIV", actual_delivery_date=None, shipment_status="Delivered")]
    clean, rej = run_ship(rows)
    r = reasons(rej)
    assert list(clean["shipment_id"]) == ["OK"]
    assert r["BADSTAT"] == "INVALID_SHIPMENT_STATUS" and r["NOWH"] == "MISSING_WAREHOUSE_ID"
    assert r["ORPH"] == "ORPHAN_WAREHOUSE_ID" and r["ORPHC"] == "ORPHAN_CUSTOMER_ID"
    assert r["BEFORE"] == "DELIVERY_BEFORE_SHIPMENT" and r["BADDATE"] == "INVALID_SHIP_OR_PROMISED_DATE"
    assert r["NODELIV"] == "MISSING_DELIVERY_DATE_FOR_DELIVERED"
    assert (rej["reject_reason"] == "DUPLICATE_SHIPMENT_ID").sum() == 1
    assert len(clean) + len(rej) == len(rows)                                              # nothing lost


def test_mixed_date_formats_standardised():
    clean, _ = run_ship([ship_row(ship_date="05/01/2026", promised_delivery_date="08-Jan-2026", actual_delivery_date="2026-01-08")])
    assert clean.loc[0, "ship_date"] == pd.Timestamp("2026-01-05") and clean.loc[0, "promised_delivery_date"] == pd.Timestamp("2026-01-08")


def test_missing_cost_is_imputed_and_flagged():
    clean, _ = run_ship([ship_row(shipping_cost=None), ship_row(shipment_id="NEG", shipping_cost="-20")])
    assert clean["cost_imputed"].all()
    assert abs(float(clean.loc[0, "shipping_cost"]) - 1000 * 0.9 * 1.0) < 0.01              # distance x rate x Standard factor


def test_in_transit_and_cancelled_keep_null_delivery():
    clean, _ = run_ship([ship_row(shipment_id="T", actual_delivery_date=None, shipment_status="In-Transit"),
                         ship_row(shipment_id="C", actual_delivery_date=None, shipment_status="canceled")])
    st = clean.set_index("shipment_id")
    assert st.loc["T", "shipment_status"] == "In Transit" and st.loc["C", "shipment_status"] == "Cancelled"
    assert st["actual_delivery_date"].isna().all() and st["is_on_time"].isna().all()


# ------------------------------------------------------------------ events & inventory
def test_events_orphans_duplicates_and_bad_timestamps():
    ships = pd.DataFrame({"shipment_id": ["S1"]})
    cols = ["event_id", "shipment_id", "event_type", "event_timestamp", "event_location", "warehouse_id", "remarks"]
    rows = [("E1", "S1", "ORDER_PICKED_UP", "2026-01-05 09:00:00", "Chicago", "WH01", None),
            ("E1", "S1", "ORDER_PICKED_UP", "2026-01-05 09:00:00", "Chicago", "WH01", None),   # duplicate id
            ("E2", "S9", "DELIVERED", "2026-01-08 10:00:00", "Denver", None, None),            # orphan
            ("E3", "S1", "LOST_IN_SPACE", "2026-01-06 10:00:00", "x", None, None),             # bad type
            ("E4", "S1", "DELIVERED", "2026-99-99 25:61:00", "Denver", None, None),            # bad timestamp
            ("E5", "S1", "DELIVERED", None, "Denver", None, None),                             # missing timestamp
            ("E6", "S1", "ARRIVED_AT_HUB", "06/01/2026 10:30", "Hub", None, None)]            # dd/mm/yyyy HH:MM
    clean, rej = clean_tracking_events(bronze(rows, cols, "shipment_tracking_events"), ships)
    assert list(clean["event_id"]) == ["E1", "E6"] and clean["event_timestamp"].notna().all()
    assert set(rej["reject_reason"]) == {"DUPLICATE_EVENT_ID", "ORPHAN_SHIPMENT_ID", "INVALID_EVENT_TYPE",
                                         "INVALID_EVENT_TIMESTAMP", "MISSING_EVENT_TIMESTAMP"}


def test_inventory_rules():
    cols = ["inventory_id", "warehouse_id", "sku", "product_name", "category", "snapshot_date", "quantity_on_hand", "reorder_level", "unit_cost"]
    row = lambda i, **k: {**dict(inventory_id=i, warehouse_id="WH01", sku="S" + i, product_name="P", category="C",  # noqa: E731
                                 snapshot_date="2026-06-30", quantity_on_hand="50", reorder_level="100", unit_cost="5"), **k}
    rows = [row("1"), row("2", quantity_on_hand="-3"), row("3", quantity_on_hand=None), row("4", warehouse_id="WH99"),
            row("5", warehouse_id=None), row("6", snapshot_date="N/A"), row("1")]
    clean, rej = clean_inventory(bronze(rows, cols, "inventory"), WH)
    assert list(clean["inventory_id"]) == ["1"]
    assert set(rej["reject_reason"]) == {"NEGATIVE_QUANTITY", "MISSING_QUANTITY", "ORPHAN_WAREHOUSE_ID",
                                         "MISSING_WAREHOUSE_ID", "INVALID_SNAPSHOT_DATE", "DUPLICATE_INVENTORY_ID"}


def test_stock_status_thresholds():
    q, r = pd.Series([40, 50, 51, 100, 101, 300]), pd.Series([100] * 6)
    assert stock_status(q, r).tolist() == ["Critical", "Critical", "Low", "Low", "Healthy", "Overstock"]


def test_delay_bucket():
    assert [delay_bucket(x) for x in [0, 1, 2, 3, 4, 9, None]] == ["On time", "1 day", "2-3 days", "2-3 days", "4+ days", "4+ days", "On time"]


# ------------------------------------------------------------------ gold
def enriched_sample():
    rows = []
    def add(i, month, partner, status, days, delay, cost, mode, cust="C1"):
        rows.append(dict(shipment_id=i, ship_month=month, partner_id=partner, partner_name="P" + partner, transport_mode=mode,
                         shipment_status=status, delivery_days=days, delay_days=delay, shipping_cost=cost, distance_km=100.0,
                         cost_per_km=cost / 100, customer_id=cust, customer_name="Cust " + cust, segment="Retail",
                         sla_breach=(days > 3) if days is not None else pd.NA, delay_reason="Weather" if status == "Delayed" else pd.NA,
                         delay_bucket="1 day" if status == "Delayed" else "On time", route_id="R1", origin_warehouse_id="WH01",
                         origin_city="Chicago", destination_city="Denver"))
    add("1", "2026-01", "A", "Delivered", 3, 0, 100.0, "Road")
    add("2", "2026-01", "A", "Delivered", 3, 0, 100.0, "Road")
    add("3", "2026-01", "A", "Delayed", 5, 2, 200.0, "Road", "C2")
    add("4", "2026-02", "B", "Delayed", 6, 3, 300.0, "Air", "C2")
    add("5", "2026-02", "B", "Cancelled", None, None, 10.0, "Air")
    add("6", "2026-02", "B", "In Transit", None, None, 50.0, "Air")
    se = pd.DataFrame(rows)
    for c in ["delivery_days", "delay_days"]:
        se[c] = se[c].astype("Int64")
    return se


def test_on_time_pct_excludes_in_transit_and_cancelled():
    perf = ga.gold_shipment_performance_summary(enriched_sample()).set_index("ship_month")
    assert perf.loc["2026-01", "on_time_delivery_pct"] == 66.67          # 2 of 3 completed
    assert perf.loc["2026-02", "on_time_delivery_pct"] == 0.0 and perf.loc["2026-02", "total_shipments"] == 3


def test_partner_ranking_and_rates():
    p = ga.gold_partner_performance(enriched_sample())
    assert p.iloc[0]["partner_id"] == "B" and p.iloc[0]["delay_rate_pct"] == 100.0 and p.iloc[0]["delay_rank"] == 1
    assert p.set_index("partner_id").loc["A", "delay_rate_pct"] == 33.33


def test_monthly_cost_mom_and_cumulative():
    c = ga.gold_monthly_cost_analysis(enriched_sample())
    assert c["total_cost"].tolist() == [400.0, 360.0] and c["cumulative_cost"].tolist() == [400.0, 760.0]
    assert pd.isna(c.loc[0, "mom_change_pct"]) and c.loc[1, "mom_change_pct"] == -10.0
    assert c.loc[0, "road_cost"] == 400.0 and c.loc[1, "air_cost"] == 360.0


def test_customer_delay_rank():
    c = ga.gold_customer_delay_analysis(enriched_sample())
    assert c.iloc[0]["customer_id"] == "C2" and c.iloc[0]["delayed_shipments"] == 2 and c.iloc[0]["delay_rank"] == 1


def test_gold_outputs_on_real_data_are_consistent():
    root = Path(__file__).resolve().parents[1] / "data"
    kpi = pd.read_csv(root / "gold" / "gold_kpi_summary.csv").set_index("metric")["value"]
    ship = pd.read_csv(root / "processed" / "shipments.csv")
    assert int(kpi["total_shipments"]) == len(ship)
    assert int(kpi["shipments_delivered_on_time"] + kpi["shipments_delayed"] + kpi["shipments_in_transit"] + kpi["shipments_cancelled"]) == len(ship)
    monthly = pd.read_csv(root / "gold" / "gold_monthly_cost_analysis.csv")
    assert abs(monthly["total_cost"].sum() - ship["shipping_cost"].sum()) < 1.0
