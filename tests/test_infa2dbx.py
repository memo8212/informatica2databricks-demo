"""Tests for the infa2dbx app (parser, codegen, expression translator)."""
from pathlib import Path

from infa2dbx.codegen import build_module, build_notebook, build_workflow, is_linear
from infa2dbx.expr import translate_expression
from infa2dbx.parser import load_repository, parse_mapping, topo_order

ROOT = Path(__file__).resolve().parents[1]
M360 = ROOT / "legacy" / "mappings" / "m_customer_360.xml"
MSCD2 = ROOT / "legacy" / "mappings" / "m_scd2_customer.xml"


def test_repository_loads_all_mappings():
    repo = load_repository(ROOT)
    names = {m.name for m in repo.mappings}
    assert {"m_load_dept_txn", "m_customer_360", "m_scd2_customer"} <= names
    assert repo.workflows and repo.workflows[0].name == "WF_DAILY_BANK_FEED"
    assert "mplt_normalize_txn" in repo.mapplets
    assert "wklt_dept_ingest" in repo.worklets
    assert repo.parameters["sections"]["s_dept_cards"]["$$DEPT"] == "CARDS"


def test_customer_360_uses_extended_transformations():
    m = parse_mapping(M360)
    types = {t.name: t.type for t in m.transforms}
    assert types["RTR_TXN"] == "Router"
    assert types["JNR"] == "Joiner"
    assert types["NORM_TXN"] == "Normalizer"
    assert types["UNI_TXN"] == "Union"
    assert types["RNK_TXN"] == "Rank"
    assert types["SRT_TXN"] == "Sorter"
    assert types["SQL_TXN"] == "SQL"
    assert not is_linear(m)


def test_notebook_generates_new_transformations():
    repo = load_repository(ROOT)
    m = next(x for x in repo.mappings if x.name == "m_customer_360")
    nb, rows = build_notebook(m, repo)
    assert 'withColumn("ROUTE"' in nb
    assert "F.rank()" in nb
    assert "F.explode(" in nb
    assert "unionByName" in nb
    assert "spark.sql(" in nb
    kinds = {r[1] for r in rows}
    assert {"Router", "Rank", "Normalizer", "Union", "SQL", "Joiner", "Sorter"} <= kinds


def test_router_condition_chaining_is_valid():
    repo = load_repository(ROOT)
    m = next(x for x in repo.mappings if x.name == "m_customer_360")
    nb, _ = build_notebook(m, repo)
    # must not produce the invalid F.when(...).F.when(...) form
    assert ".F.when(" not in nb
    assert '.when((F.col("BAND") == "LOW"), "G_LOW")' in nb


def test_linear_mapping_generates_runnable_module():
    repo = load_repository(ROOT)
    m = next(x for x in repo.mappings if x.name == "m_load_dept_txn")
    assert is_linear(m)
    src = build_module(m, repo)
    assert "def run(spark, bridge):" in src
    assert 'spark.read.parquet(f"{bridge}/dept_txn")' in src
    assert "groupBy(" in src


def test_scd2_mapping_recognized():
    m = parse_mapping(MSCD2)
    types = {t.name: t.type for t in m.transforms}
    assert types["LKP_CURRENT"] == "Lookup Procedure"
    assert types["EXP_SCD2"] == "Expression"
    assert types["UPD_STRAT"] == "Update Strategy"
    order = [t.name for t in topo_order(m)]
    assert order[0] == "SQ_CUST"


def test_expression_translator_extras():
    assert translate_expression("IN(DEPT, 'CARDS', 'LOANS')") == \
        '(F.col("DEPT").isin("CARDS", "LOANS"))'
    assert translate_expression("TO_CHAR(TXN_DATE, 'yyyyMM')") == \
        "F.date_format(F.col(\"TXN_DATE\"), \"yyyyMM\")"
    assert translate_expression("NVL(FX_RATE, 1)") == \
        'F.coalesce(F.col("FX_RATE"), 1)'
    assert translate_expression("GREATEST(A, B)") == \
        'F.greatest(F.col("A"), F.col("B"))'


def test_workflow_from_repository():
    repo = load_repository(ROOT)
    job = build_workflow(repo.workflows[0])
    assert job["name"] == "WF_DAILY_BANK_FEED"
    keys = {t["task_key"] for t in job["tasks"]}
    assert {"wklt_dept_ingest", "s_gold_summary", "email_notify"} <= keys
    pnames = {p["name"] for p in job["parameters"]}
    assert "$$BATCH_ID" in pnames
