# Test Report

_Generated 2026-11-15 12:01 UTC_

**Runner:** built-in fallback runner (`scripts/run_tests.py`) - `pytest` could not be installed in the build environment (no network). The tests are plain `test_*` functions with `assert`, so `pytest -v` runs them unchanged.

**Result: 40 passed, 0 failed, 40 total**

| File | Test | Result | Seconds | Note |
|---|---|---|---:|---|
| test_data_quality.py | test_bronze_data_contains_the_planted_issues | PASSED | 0.84 |  |
| test_data_quality.py | test_bronze_equals_silver_plus_rejected | PASSED | 0.44 |  |
| test_data_quality.py | test_delivery_before_ship_detected | PASSED | 0.01 |  |
| test_data_quality.py | test_duplicate_shipment_ids_detected | PASSED | 0.0 |  |
| test_data_quality.py | test_invalid_dates_become_nat_and_are_flagged | PASSED | 0.0 |  |
| test_data_quality.py | test_invalid_status_detected_with_normalisation | PASSED | 0.0 |  |
| test_data_quality.py | test_missing_value_report_counts | PASSED | 0.0 |  |
| test_data_quality.py | test_negative_inventory_detected | PASSED | 0.0 |  |
| test_data_quality.py | test_not_null_counts_blank_and_whitespace | PASSED | 0.0 |  |
| test_data_quality.py | test_not_null_warn_severity_does_not_fail | PASSED | 0.0 |  |
| test_data_quality.py | test_parse_mixed_dates_supports_three_formats | PASSED | 0.0 |  |
| test_data_quality.py | test_parse_timestamps_mixed | PASSED | 0.01 |  |
| test_data_quality.py | test_referential_integrity_flags_orphans_only | PASSED | 0.0 |  |
| test_data_quality.py | test_silver_data_passes_all_error_level_checks | PASSED | 0.59 |  |
| test_data_quality.py | test_unexpected_data_type_detected | PASSED | 0.0 |  |
| test_data_quality.py | test_valid_dates_check | PASSED | 0.0 |  |
| test_project_assets.py | test_adf_expression_balance_helper | PASSED | 0.0 |  |
| test_project_assets.py | test_adf_has_required_activity_types | PASSED | 0.0 |  |
| test_project_assets.py | test_adf_json_files_pass_static_validation | PASSED | 0.0 |  |
| test_project_assets.py | test_adf_validator_catches_a_broken_reference | PASSED | 0.01 |  |
| test_project_assets.py | test_all_required_files_exist_and_are_not_empty | PASSED | 0.0 |  |
| test_project_assets.py | test_generated_data_is_interconnected_and_dirty | PASSED | 0.03 |  |
| test_project_assets.py | test_generator_is_reproducible_with_a_seed | PASSED | 0.35 |  |
| test_project_assets.py | test_no_secrets_in_repository | PASSED | 0.01 |  |
| test_project_assets.py | test_notebooks_are_valid_nbformat4_with_valid_python | PASSED | 0.01 |  |
| test_project_assets.py | test_sql_lint_passes_and_has_enough_queries | PASSED | 0.01 |  |
| test_transformations.py | test_customer_delay_rank | PASSED | 0.02 |  |
| test_transformations.py | test_delay_bucket | PASSED | 0.0 |  |
| test_transformations.py | test_events_orphans_duplicates_and_bad_timestamps | PASSED | 0.03 |  |
| test_transformations.py | test_gold_outputs_on_real_data_are_consistent | PASSED | 0.04 |  |
| test_transformations.py | test_in_transit_and_cancelled_keep_null_delivery | PASSED | 0.04 |  |
| test_transformations.py | test_inventory_rules | PASSED | 0.05 |  |
| test_transformations.py | test_missing_cost_is_imputed_and_flagged | PASSED | 0.05 |  |
| test_transformations.py | test_mixed_date_formats_standardised | PASSED | 0.05 |  |
| test_transformations.py | test_monthly_cost_mom_and_cumulative | PASSED | 0.02 |  |
| test_transformations.py | test_on_time_pct_excludes_in_transit_and_cancelled | PASSED | 0.02 |  |
| test_transformations.py | test_partner_ranking_and_rates | PASSED | 0.03 |  |
| test_transformations.py | test_rejection_rules_one_reason_each | PASSED | 0.09 |  |
| test_transformations.py | test_status_is_derived_from_dates_not_trusted | PASSED | 0.04 |  |
| test_transformations.py | test_stock_status_thresholds | PASSED | 0.0 |  |
