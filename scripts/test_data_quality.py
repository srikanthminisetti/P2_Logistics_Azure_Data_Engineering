"""Unit + integration tests for the data-quality checks (run with:  pytest -v)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.utils.config import BRONZE_DIR, SILVER_DIR, VALID_SHIPMENT_STATUSES
from src.utils.date_utils import invalid_date_mask, parse_mixed_dates, parse_mixed_timestamps
from src.validation import data_quality as dq
from src.validation.profiling import duplicate_summary, missing_value_report


def S(values):
    return pd.Series(values, dtype="string")


# ------------------------------------------------------------------ date standardisation
def test_parse_mixed_dates_supports_three_formats():
    out = parse_mixed_dates(S(["2026-03-15", "15/03/2026", "15-Mar-2026"]))
    assert out.nunique() == 1 and out.iloc[0] == pd.Timestamp("2026-03-15")


def test_invalid_dates_become_nat_and_are_flagged():
    raw = S(["2026-13-45", "31/02/2026", "N/A", None, "2026-01-02"])
    parsed = parse_mixed_dates(raw)
    assert parsed.isna().tolist() == [True, True, True, True, False]
    assert invalid_date_mask(raw, parsed).tolist() == [True, True, True, False, False]   # NULL is not "invalid"


def test_parse_timestamps_mixed():
    out = parse_mixed_timestamps(S(["2026-03-15 09:30:00", "15/03/2026 09:30", "2026-03-15T09:30:00", "2026-99-99 25:61:00"]))
    assert out.iloc[:3].nunique() == 1 and pd.isna(out.iloc[3])


# ------------------------------------------------------------------ individual checks
def test_not_null_counts_blank_and_whitespace():
    df = pd.DataFrame({"warehouse_id": S(["WH01", None, "  ", ""])})
    r = dq.check_not_null(df, "warehouse_id", "t", "x")
    assert r["rows_failed"] == 3 and r["status"] == "FAIL"


def test_not_null_warn_severity_does_not_fail():
    df = pd.DataFrame({"email": S([None, "a@b.com"])})
    assert dq.check_not_null(df, "email", severity="WARN")["status"] == "WARN"


def test_duplicate_shipment_ids_detected():
    df = pd.DataFrame({"shipment_id": S(["SHP1", "SHP2", "SHP1", "SHP1"])})
    r = dq.check_unique_key(df, "shipment_id")
    assert r["rows_failed"] == 2   # 2 extra copies of SHP1
    assert duplicate_summary(df, "shipment_id")["duplicate_groups"] == 1


def test_valid_dates_check():
    df = pd.DataFrame({"ship_date": S(["2026-01-01", "2026-02-30", "garbage", None])})
    assert dq.check_valid_dates(df, "ship_date")["rows_failed"] == 2


def test_referential_integrity_flags_orphans_only():
    df = pd.DataFrame({"origin_warehouse_id": S(["WH01", "WH99", None, "WH02"])})
    r = dq.check_referential_integrity(df, "origin_warehouse_id", ["WH01", "WH02"], parent_name="warehouses")
    assert r["rows_failed"] == 1 and r["sample_values"] == "WH99"       # NULLs are the not_null check's job


def test_invalid_status_detected_with_normalisation():
    df = pd.DataFrame({"shipment_status": S(["Delivered", " delayed ", "UNKNOWN", "N/A"])})
    r = dq.check_allowed_values(df, "shipment_status", VALID_SHIPMENT_STATUSES, normalise=lambda s: s.str.title())
    assert r["rows_failed"] == 2


def test_negative_inventory_detected():
    df = pd.DataFrame({"quantity_on_hand": S(["10", "-5", "0", "-1"])})
    assert dq.check_non_negative(df, "quantity_on_hand")["rows_failed"] == 2


def test_delivery_before_ship_detected():
    df = pd.DataFrame({"ship_date": S(["2026-01-10", "2026-01-10", "2026-01-10"]),
                       "actual_delivery_date": S(["2026-01-09", "2026-01-12", None])})
    assert dq.check_delivery_after_ship(df)["rows_failed"] == 1


def test_unexpected_data_type_detected():
    df = pd.DataFrame({"weight_kg": S(["12.5", "N/A", "12kg", None])})
    assert dq.check_numeric_type(df, "weight_kg")["rows_failed"] == 2


def test_missing_value_report_counts():
    df = pd.DataFrame({"a": S(["x", None, ""]), "b": S(["1", "2", "3"])})
    rep = missing_value_report(df, "t").set_index("column")
    assert rep.loc["a", "missing_count"] == 2 and rep.loc["b", "missing_count"] == 0


# ------------------------------------------------------------------ integration on the generated data
def _load(folder):
    names = ["customers", "warehouses", "transportation_partners", "shipments", "shipment_tracking_events", "inventory"]
    return {n: pd.read_csv(Path(folder) / f"{n}.csv", dtype="string", low_memory=False) for n in names}


def test_bronze_data_contains_the_planted_issues():
    rep = dq.run_suite(_load(BRONZE_DIR), "bronze")
    failed = rep[rep["rows_failed"] > 0]
    got = set(zip(failed["table"], failed["check_name"]))
    for expected in [("shipments", "unique_key"), ("shipments", "allowed_values"), ("shipments", "delivery_date_order"),
                     ("inventory", "non_negative"), ("shipment_tracking_events", "valid_dates")]:
        assert expected in got, f"expected data-quality issue not detected: {expected}"


def test_silver_data_passes_all_error_level_checks():
    rep = dq.run_suite(_load(SILVER_DIR), "silver")
    bad = rep[rep["status"] == "FAIL"]
    assert bad.empty, bad[["table", "check_name", "column", "rows_failed"]].to_string()


def test_bronze_equals_silver_plus_rejected():
    bronze, silver = _load(BRONZE_DIR), _load(SILVER_DIR)
    rejected = pd.read_csv(SILVER_DIR / "rejected_records.csv", dtype="string")
    rec = dq.reconcile_counts(bronze, silver, rejected)
    assert (rec["status"] == "PASS").all(), rec.to_string()
