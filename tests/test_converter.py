"""Unit tests for the PowerCenter converter + expression translator."""
from pathlib import Path

from converter.infa_expr import translate_expression
from converter.pc2dbx_converter import (
    parse_mapping, topo_order, build_workflow, build_parameters, build_notebook,
)

ROOT = Path(__file__).resolve().parents[1]
MAPPING = ROOT / "legacy" / "mappings" / "m_load_dept_txn.xml"


def test_expression_translation():
    assert translate_expression("UPPER(DEPT_CODE)") == 'F.upper(F.col("DEPT_CODE"))'
    assert translate_expression("ROUND(AMOUNT * FX_RATE, 2)") == \
        'F.round((F.col("AMOUNT") * F.col("FX_RATE")), 2)'
    iif = translate_expression("IIF(STATUS = 'REVERSAL', 'Y', 'N')")
    assert "F.when(" in iif and ".otherwise(" in iif and '"REVERSAL"' in iif
    flt = translate_expression("IS_REV = 'N' AND AMOUNT_BASE <> 0")
    assert "&" in flt and "!=" in flt
    dec = translate_expression("DECODE(SEGMENT, 'A', 1, 'B', 2, 0)")
    assert dec.count("when(") == 2 and dec.endswith(".otherwise(0)")


def test_parse_mapping_structure():
    m = parse_mapping(MAPPING)
    assert m["name"] == "m_load_dept_txn"
    types = {t["name"]: t["type"] for t in m["transforms"]}
    assert types["SQ_DEPT_TXN"] == "Source Qualifier"
    assert types["AGG_DAILY"] == "Aggregator"
    assert types["UPD_STRAT"] == "Update Strategy"
    assert len(m["connectors"]) > 20


def test_topo_order_starts_with_source_qualifier():
    order = topo_order(parse_mapping(MAPPING))
    names = [t["name"] for t in order]
    assert names[0] == "SQ_DEPT_TXN"
    assert names.index("EXP_NORMALIZE") < names.index("FILT_ACTIVE") < names.index("AGG_DAILY")
    assert names[-1] == "UPD_STRAT"


def test_workflow_tasks_and_links():
    job, tasks = build_workflow()
    assert job["name"] == "WF_DAILY_BANK_FEED"
    keys = {t["task_key"] for t in job["tasks"]}
    assert {"wklt_dept_ingest", "s_gold_summary", "email_notify"} <= keys
    # dependency chain start -> worklet -> decision -> command -> session -> email
    by_key = {t["task_key"]: t for t in job["tasks"]}
    assert by_key["dec_rowcount"]["depends_on"] == [{"task_key": "wklt_dept_ingest"}]
    # workflow variables become parameters
    pnames = {p["name"] for p in job["parameters"]}
    assert "$$BATCH_ID" in pnames and "$$BASE_CCY" in pnames


def test_parameter_file_parsing():
    params = build_parameters()
    assert params["global"]["$$BASE_CCY"] == "CAD"
    assert params["sections"]["s_dept_cards"]["$$DEPT"] == "CARDS"
    assert params["sections"]["s_dept_wealth"]["$$DEPT"] == "WEALTH"


def test_notebook_generation_has_translated_ops():
    nb, rows = build_notebook(parse_mapping(MAPPING))
    assert "F.upper(F.col(\"DEPT_CODE\"))" in nb
    assert "groupBy(" in nb and ".agg(" in nb
    assert "monotonically_increasing_id" in nb
    assert any(r[1] == "Update Strategy" for r in rows)
