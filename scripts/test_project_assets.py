"""Tests for generator reproducibility, ADF JSON, SQL scripts, notebooks and project completeness."""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from src.data_generation.generate_data import generate
from src.utils.config import ROOT
from src.validation import lint_sql, validate_adf_json as adf

REQUIRED = [
    "README.md", "requirements.txt", ".gitignore", "LICENSE", "architecture/architecture.md", "architecture/architecture_diagram.png",
    "data/data_dictionary.md", "data/raw/shipments.csv", "data/raw/shipment_tracking_events.csv", "data/raw/warehouses.csv",
    "data/raw/inventory.csv", "data/raw/transportation_partners.csv", "data/raw/customers.csv",
    "notebooks/01_bronze_ingestion.ipynb", "notebooks/02_silver_data_cleaning.ipynb", "notebooks/03_shipment_transformation.ipynb",
    "notebooks/04_inventory_transformation.ipynb", "notebooks/05_gold_analytics.ipynb", "notebooks/06_data_quality_validation.ipynb",
    "sql/01_database_setup.sql", "sql/02_table_creation.sql", "sql/03_analytics_queries.sql", "sql/04_stored_procedures.sql",
    "tests/test_data_quality.py", "tests/test_transformations.py", "results/execution_summary.md", "results/data_quality_report.csv",
    "results/shipment_analysis.csv", "results/inventory_analysis.csv", "screenshots/README.md",
    "docs/project_overview.md", "docs/business_requirements.md", "docs/implementation_guide.md", "docs/azure_setup_guide.md",
    "docs/pipeline_explanation.md", "docs/interview_questions.md", "docs/troubleshooting.md",
]


def test_generator_is_reproducible_with_a_seed():
    with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
        generate(seed=7, n_shipments=300, out_dir=a)
        generate(seed=7, n_shipments=300, out_dir=b)
        for f in Path(a).glob("*.csv"):
            assert f.read_bytes() == (Path(b) / f.name).read_bytes(), f.name


def test_generated_data_is_interconnected_and_dirty():
    d = ROOT / "data" / "raw"
    ship = pd.read_csv(d / "shipments.csv", dtype="string")
    cust = pd.read_csv(d / "customers.csv", dtype="string")
    assert len(ship) > 10000 and ship["shipment_id"].duplicated().any()          # enough data + planted duplicates
    assert ship["customer_id"].isin(cust["customer_id"]).mean() > 0.98           # relationships hold for >98%
    assert ship["ship_date"].str.contains("/").any() and ship["ship_date"].str.match(r"^\d{2}-[A-Za-z]{3}-").any()  # mixed formats


def test_adf_json_files_pass_static_validation():
    errors = [i for i in adf.validate() if i[0] == "ERROR"]
    assert not errors, errors


def test_adf_validator_catches_a_broken_reference(tmp_path=None):
    with tempfile.TemporaryDirectory() as t:
        import shutil
        shutil.copytree(ROOT / "adf", Path(t) / "adf")
        p = Path(t) / "adf" / "pipelines" / "PL_Master_Logistics.json"
        doc = json.loads(p.read_text())
        doc["properties"]["activities"][1]["dependsOn"][0]["activity"] = "DOES_NOT_EXIST"
        p.write_text(json.dumps(doc))
        assert any("unknown" in m for _, _, m in adf.validate(Path(t) / "adf"))


def test_adf_expression_balance_helper():
    assert adf.expression_balanced("@concat('a', pipeline().RunId)")
    assert not adf.expression_balanced("@concat('a', pipeline().RunId")
    assert adf.expression_balanced("@concat('it''s', 'x')")


def test_adf_has_required_activity_types():
    types = set()
    for f in (ROOT / "adf" / "pipelines").glob("*.json"):
        for a in adf.walk_activities(json.loads(f.read_text())["properties"]["activities"]):
            types.add(a["type"])
    assert {"Copy", "ForEach", "DatabricksNotebook", "Lookup", "Fail", "WebActivity", "ExecutePipeline"} <= types


def test_sql_lint_passes_and_has_enough_queries():
    issues, n_queries, _ = lint_sql.lint()
    assert not issues, issues
    assert n_queries >= 15


def test_notebooks_are_valid_nbformat4_with_valid_python():
    import ast
    for f in sorted((ROOT / "notebooks").glob("*.ipynb")):
        nb = json.loads(f.read_text())
        assert nb["nbformat"] == 4 and nb["cells"]
        code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
        ast.parse(code)


def test_all_required_files_exist_and_are_not_empty():
    missing = [p for p in REQUIRED if not (ROOT / p).exists() or (ROOT / p).stat().st_size == 0]
    assert not missing, missing


def test_no_secrets_in_repository():
    import re
    pat = re.compile(r"(AccountKey=|SharedAccessSignature=|-----BEGIN|dapi[a-f0-9]{24,})")
    for f in ROOT.rglob("*"):
        if f.suffix in (".json", ".md", ".py", ".sql", ".yml", ".txt") and f.is_file() and ".git" not in f.parts and f.name != "test_project_assets.py" and f.name != "validate_adf_json.py":
            assert not pat.search(f.read_text(errors="ignore")), f
