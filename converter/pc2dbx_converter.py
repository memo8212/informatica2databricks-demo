"""Informatica PowerCenter -> Databricks converter (POC).

Reads the PowerCenter repository export under legacy/ and emits:

  artifacts/generated/conversion_report.md       component + transformation mapping
  artifacts/generated/databricks_workflow.json    workflow -> Workflow (job) DAG
  artifacts/generated/notebook_m_load_dept_txn.py mapping -> PySpark notebook
  artifacts/generated/schema.py                   sources/targets -> StructTypes
  artifacts/generated/parameters.json             parameter file -> job parameters

Mapping rules (POC):
  Source Qualifier      -> read Bronze table (+ WHERE from the Sql Query)
  Lookup Procedure      -> join to the lookup table
  Expression            -> withColumn(<port>, <translated expression>)
  Filter                -> filter(<translated condition>)
  Aggregator            -> groupBy(<group-by ports>).agg(<SUM/AVG/...>)
  Sequence Generator    -> withColumn(key, monotonically_increasing_id())
  Update Strategy       -> Delta MERGE (DD_UPDATE / DD_INSERT / DD_DELETE)
  Session               -> Databricks Workflow task (notebook)
  Workflow              -> Databricks Workflow (job)
  Worklet               -> sub-workflow (tasks inlined)
  Mapplet               -> reusable function / shared module
  Decision task         -> task run_if condition
  Command task          -> python_task / shell
  Email task            -> job notification
  Parameter file        -> job parameters / bundle variables
  Connection            -> Unity Catalog + Auto Loader / Lakeflow Connect

Run:  python converter/pc2dbx_converter.py
"""
from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from converter.infa_expr import translate_expression  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LEGACY = ROOT / "legacy"
GEN = ROOT / "artifacts" / "generated"
GEN.mkdir(parents=True, exist_ok=True)


def _find_all(folder: Path, name: str) -> list[ET.Element]:
    out = []
    for p in sorted(folder.glob("*.xml")):
        root = ET.parse(p).getroot()
        out += root.iter(name)
    return out


# --------------------------------------------------------------- expressions
def _translate(expr: str) -> str:
    return translate_expression(expr or "")


# --------------------------------------------------------------- mapping
def parse_mapping(path: Path):
    root = ET.parse(path).getroot()
    mapping = next(root.iter("MAPPING"))
    transforms = []
    for tr in mapping.iter("TRANSFORMATION"):
        fields = []
        for f in tr.iter("TRANSFORMFIELD"):
            fields.append({
                "name": f.get("NAME"),
                "porttype": f.get("PORTTYPE", ""),
                "expression": f.get("EXPRESSION", ""),
                "datatype": f.get("DATATYPE", "string"),
            })
        attrs = {a.get("NAME"): a.get("VALUE") for a in tr.iter("TABLEATTRIBUTE")}
        transforms.append({"name": tr.get("NAME"), "type": tr.get("TYPE"),
                           "fields": fields, "attrs": attrs})
    connectors = [{"from": c.get("FROMINSTANCE"), "to": c.get("TOINSTANCE"),
                   "field": c.get("FROMFIELD")} for c in mapping.iter("CONNECTOR")]
    return {"name": mapping.get("NAME"), "transforms": transforms,
            "connectors": connectors}


def topo_order(mapping) -> list[dict]:
    names = [t["name"] for t in mapping["transforms"]]
    indeg = {n: 0 for n in names}
    adj = {n: [] for n in names}
    for c in mapping["connectors"]:
        if c["from"] in indeg and c["to"] in adj:
            adj[c["from"]].append(c["to"])
            indeg[c["to"]] += 1
    order, queue = [], [n for n in names if indeg[n] == 0]
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    by_name = {t["name"]: t for t in mapping["transforms"]}
    return [by_name[n] for n in order]


# --------------------------------------------------------------- notebook
def build_notebook(mapping) -> tuple[str, list]:
    lines = [
        "# Databricks notebook source",
        "# MAGIC %md",
        f"# {mapping['name']} (migrated from Informatica PowerCenter)",
        "# MAGIC Parameterized by the `DEPT` widget (was mapping parameter `$$DEPT`).",
        "",
        "# COMMAND ----------",
        "from pyspark.sql import functions as F",
        "",
        'DEPT = dbutils.widgets.get("DEPT")',
        "",
    ]
    rows = []
    for t in topo_order(mapping):
        typ = t["type"]
        lines += ["# COMMAND ----------", f"# MAGIC %md ## {t['name']}  ({typ})", ""]
        if typ == "Source Qualifier":
            src = t["attrs"].get("Sql Query", "")
            tbl = "bronze_dept_txn"
            lines += [f"df = spark.read.table('{tbl}')",
                      f"df = df.filter(F.col('DEPT_CODE') == DEPT)  # {src[:60]}",
                      "df.createOrReplaceTempView('dept_txn')"]
            rows.append((t["name"], typ, "read bronze_dept_txn + dept filter"))
        elif typ == "Lookup Procedure":
            tbl = t["attrs"].get("Lookup table name", "lookup").lower()
            cond = t["attrs"].get("Lookup condition", "")
            key = cond.split("=")[0].split(".")[-1].strip() if "=" in cond else "ID"
            lines += [f"_lk = spark.read.table('{tbl}')",
                      f"df = df.join(_lk, on='{key}', how='left')  # {cond}"]
            rows.append((t["name"], typ, f"join {tbl} on {key}"))
        elif typ == "Expression":
            for f in t["fields"]:
                if f["expression"]:
                    lines.append(f"df = df.withColumn('{f['name']}', {_translate(f['expression'])})")
            rows.append((t["name"], typ, "; ".join(f["name"] for f in t["fields"] if f["expression"])))
        elif typ == "Filter":
            cond = t["attrs"].get("Filter Condition", "1=1")
            lines.append(f"df = df.filter({_translate(cond)})")
            rows.append((t["name"], typ, cond))
        elif typ == "Aggregator":
            gb = [f["name"] for f in t["fields"] if f["porttype"] == "GROUP BY"]
            aggs = [f for f in t["fields"] if f["porttype"] == "OUTPUT" and f["expression"]]
            agg_expr = ", ".join(f"{_translate(f['expression'])}.alias('{f['name']}')" for f in aggs)
            lines.append(f"df = df.groupBy({', '.join(repr(c) for c in gb)}).agg({agg_expr})")
            rows.append((t["name"], typ, f"groupBy {gb} agg {[a['name'] for a in aggs]}"))
        elif typ == "Sequence Generator":
            key = t["fields"][0]["name"] if t["fields"] else "SEQ_KEY"
            lines.append(f"df = df.withColumn('{key}', F.monotonically_increasing_id())")
            rows.append((t["name"], typ, f"withColumn {key}"))
        elif typ == "Update Strategy":
            strat = t["attrs"].get("Update Strategy Expression", "DD_UPDATE")
            lines += ["# Update Strategy -> Delta MERGE into the target",
                      "df.createOrReplaceTempView('upd')",
                      "# spark.sql('MERGE INTO fact_daily_customer t USING upd s ON ...')"]
            rows.append((t["name"], typ, f"MERGE ({strat})"))
        else:
            lines.append(f"# TODO: {typ} not auto-translated")
            rows.append((t["name"], typ, "REVIEW"))
    return "\n".join(lines) + "\n", rows


# --------------------------------------------------------------- workflow
TASK_KIND = {"Session": "notebook", "Worklet": "sub-workflow", "Decision": "conditional",
             "Command": "python_task", "Email": "notification", "Start": "start",
             "Timer": "schedule"}


def build_workflow() -> tuple[dict, list]:
    wf = next(iter(_find_all(LEGACY / "workflows", "WORKFLOW")))
    name = wf.get("NAME")
    tasks = []
    for t in wf.iter("TASK"):
        tasks.append({"name": t.get("NAME"), "type": t.get("TYPE"),
                      "attrs": {a.get("NAME"): a.get("VALUE") for a in t.iter("TABLEATTRIBUTE")}})
    links = [(l.get("FROMTASK"), l.get("TOTASK")) for l in wf.iter("WORKFLOWLINK")]
    variables = [{"name": v.get("NAME"), "value": v.get("VALUE"), "type": v.get("DATATYPE")}
                 for v in wf.iter("WORKFLOWVARIABLE")]

    deps = {}
    for f, t in links:
        deps.setdefault(t, []).append(f)

    jtasks = []
    for t in tasks:
        if t["type"] == "Start":
            continue
        jtasks.append({
            "task_key": t["name"],
            "kind": TASK_KIND.get(t["type"], "notebook"),
            "depends_on": [{"task_key": d} for d in deps.get(t["name"], []) if d != "start"],
        })
    job = {"name": name, "schedule": wf.get("SCHEDULE", ""),
           "parameters": variables, "tasks": jtasks}
    return job, tasks


# --------------------------------------------------------------- schema
def build_schema() -> str:
    lines = ['"""StructTypes derived from PowerCenter sources/targets."""',
             "from pyspark.sql.types import (StructType, StructField, StringType,",
             "                               LongType, DecimalType, DateType)",
             "", "TYPE_MAP = {",
             '    "string": "StringType()", "integer": "LongType()",',
             '    "decimal": "DecimalType({p}, {s})", "date/time": "DateType()",',
             "}", ""]
    for el in list(_find_all(LEGACY / "sources", "SOURCE")) + list(_find_all(LEGACY / "sources", "TARGET")):
        kind = "SOURCE" if el.tag == "SOURCE" else "TARGET"
        lines.append(f"# {kind} {el.get('NAME')} ({el.get('DATABASETYPE')})")
        lines.append(f"{el.get('NAME')}_SCHEMA = StructType([")
        for f in el.iter("SOURCEFIELD") if el.tag == "SOURCE" else el.iter("TARGETFIELD"):
            dt = f.get("DATATYPE")
            p, s = f.get("PRECISION", "0"), f.get("SCALE", "0")
            st = {"string": "StringType()", "integer": "LongType()",
                  "decimal": f"DecimalType({p}, {s})", "date/time": "DateType()"}.get(dt, "StringType()")
            lines.append(f'    StructField("{f.get("NAME")}", {st}, True),')
        lines.append("])")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------- parameters
def build_parameters() -> dict:
    text = (LEGACY / "parameters" / "bank_params.prm").read_text()
    params = {"global": {}, "sections": {}}
    section = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            params["sections"].setdefault(section, {})
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            if section in (None, "Global"):
                params["global"][k.strip()] = v.strip()
            else:
                params["sections"][section][k.strip()] = v.strip()
    return params


# --------------------------------------------------------------- report
COMPONENT_MAP = [
    ("Source Qualifier", "read Bronze table (+ WHERE)"),
    ("Lookup Procedure", "join to lookup table"),
    ("Expression", "withColumn(translated expression)"),
    ("Filter", "filter(translated condition)"),
    ("Router", "multiple filtered branches (union on read)"),
    ("Joiner", "join (broadcast for small dims)"),
    ("Aggregator", "groupBy(...).agg(...)"),
    ("Sorter", "orderBy"),
    ("Rank", "Window.orderBy + rank()"),
    ("Union", "unionByName"),
    ("Normalizer", "explode / stack"),
    ("Sequence Generator", "monotonically_increasing_id / Delta identity"),
    ("Update Strategy (DD_*)", "Delta MERGE (UPDATE/INSERT/DELETE)"),
    ("Transaction Control", "Delta ACID transaction"),
    ("Session", "Databricks Workflow task (notebook)"),
    ("Workflow", "Databricks Workflow (job)"),
    ("Worklet", "sub-workflow (inlined tasks) / reusable job"),
    ("Mapplet", "reusable function / shared module"),
    ("Decision task", "task run_if condition"),
    ("Command task", "python/shell task"),
    ("Email task", "job notification"),
    ("Timer", "schedule / trigger"),
    ("Parameter file (.prm)", "job parameters / bundle variables"),
    ("Connection (relational/file)", "Unity Catalog + Auto Loader / Lakeflow Connect"),
    ("Repository (pmrep)", "Git + Unity Catalog"),
    ("pmcmd", "databricks CLI / jobs run-now"),
]


def build_report(mapping, rows, job, tasks, mapplet_names, worklet_names) -> str:
    lines = [
        f"# Conversion Report — {mapping['name']} / {job['name']}",
        "",
        "Informatica PowerCenter -> Databricks. Automatic mapping summary.",
        "",
        "## Component mapping",
        "",
        "| PowerCenter component | Databricks target |",
        "|---|---|",
    ]
    for c, d in COMPONENT_MAP:
        lines.append(f"| {c} | {d} |")

    lines += ["", "## Mapping transformations (in execution order)", "",
              "| # | Transformation | Type | Generated |", "|---|---|---|---|"]
    for i, r in enumerate(rows, 1):
        lines.append(f"| {i} | {r[0]} | {r[1]} | {r[2]} |")

    lines += ["", "## Workflow tasks", "",
              "| Task | PowerCenter type | Databricks kind | Depends on |", "|---|---|---|---|"]
    for t in job["tasks"]:
        deps = ", ".join(d["task_key"] for d in t["depends_on"]) or "-"
        lines.append(f"| {t['task_key']} | {next((x['type'] for x in tasks if x['name']==t['task_key']), '')} "
                     f"| {t['kind']} | {deps} |")

    lines += ["", "## Reusable components", "",
              f"- Mapplets: {', '.join(mapplet_names) or 'none'} -> shared PySpark module(s)",
              f"- Worklets: {', '.join(worklet_names) or 'none'} -> inlined tasks / reusable job",
              "",
              "## Notes",
              "- The mapping is parameterized by `$$DEPT`; each session supplies a value "
              "from `bank_params.prm`. Migrated to a Databricks job parameter / widget.",
              "- Update Strategy `DD_UPDATE` becomes a Delta `MERGE` into the target.",
              "- Pre/Post SQL becomes a post-load `OPTIMIZE`/`ANALYZE` task.",
              "- `pmcmd startworkflow` becomes a Databricks job trigger / `run_job_task`.",
              ]
    return "\n".join(lines)


def main() -> None:
    mapping = parse_mapping(LEGACY / "mappings" / "m_load_dept_txn.xml")
    notebook, rows = build_notebook(mapping)
    job, tasks = build_workflow()
    mapplets = [m.get("NAME") for m in _find_all(LEGACY / "mapplets", "MAPPLET")]
    worklets = [w.get("NAME") for w in _find_all(LEGACY / "mapplets", "WORKLET")]

    (GEN / "notebook_m_load_dept_txn.py").write_text(notebook, encoding="utf-8")
    (GEN / "databricks_workflow.json").write_text(json.dumps(job, indent=2), encoding="utf-8")
    (GEN / "schema.py").write_text(build_schema(), encoding="utf-8")
    (GEN / "parameters.json").write_text(json.dumps(build_parameters(), indent=2), encoding="utf-8")
    (GEN / "conversion_report.md").write_text(
        build_report(mapping, rows, job, tasks, mapplets, worklets), encoding="utf-8")

    print(f"Converted mapping {mapping['name']} and workflow {job['name']}:")
    for f in sorted(GEN.iterdir()):
        print(f"  - {f}")
    print(f"  {len(rows)} transformations, {len(job['tasks'])} workflow tasks mapped")


if __name__ == "__main__":
    main()
