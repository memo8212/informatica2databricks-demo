"""Conversion report (markdown)."""
from __future__ import annotations

from infa2dbx.parser import Mapping, Repository

COMPONENT_MAP = [
    ("Source Qualifier", "read Bronze table (+ WHERE)"),
    ("Lookup Procedure", "join to lookup table (broadcast small dims)"),
    ("Expression", "withColumn(translated expression)"),
    ("Filter", "filter(translated condition)"),
    ("Router", "ROUTE column + per-group views / branches"),
    ("Joiner", "join (inner/left/right/full)"),
    ("Aggregator", "groupBy(...).agg(...)"),
    ("Sorter", "orderBy"),
    ("Rank", "Window.orderBy + rank() + top-N filter"),
    ("Union", "unionByName(allowMissingColumns=True)"),
    ("Normalizer", "explode / stack"),
    ("Sequence Generator", "monotonically_increasing_id / Delta identity"),
    ("Update Strategy (DD_*)", "Delta MERGE (UPDATE/INSERT/DELETE)"),
    ("SQL transformation", "spark.sql"),
    ("Session", "Workflow task (notebook)"),
    ("Workflow", "Databricks Workflow (job)"),
    ("Worklet", "sub-workflow / inlined tasks"),
    ("Mapplet", "reusable function / shared module"),
    ("Decision task", "task run_if"),
    ("Command task", "python/shell task"),
    ("Email task", "job notification"),
    ("Timer", "schedule / trigger"),
    ("Parameter file (.prm)", "job parameters / bundle variables"),
    ("Connection", "Unity Catalog + Auto Loader / Lakeflow Connect"),
    ("Repository (pmrep)", "Git + Unity Catalog"),
    ("pmcmd", "databricks CLI / jobs run-now"),
]


def build_report(repo: Repository, mapping: Mapping, rows: list, job: dict) -> str:
    lines = [
        f"# Conversion Report — {mapping.name}",
        "",
        "Informatica PowerCenter -> Databricks. Automatic mapping summary.",
        "",
        "## Component mapping",
        "",
        "| PowerCenter component | Databricks target |",
        "|---|---|",
    ]
    lines += [f"| {c} | {d} |" for c, d in COMPONENT_MAP]

    lines += ["", "## Mapping transformations (execution order)", "",
              "| # | Transformation | Type | Generated |", "|---|---|---|---|"]
    for i, (name, typ, gen) in enumerate(rows, 1):
        lines.append(f"| {i} | {name} | {typ} | {gen} |")

    if job:
        lines += ["", "## Workflow tasks", "",
                  "| Task | Kind | Depends on |", "|---|---|---|"]
        for t in job["tasks"]:
            deps = ", ".join(d["task_key"] for d in t["depends_on"]) or "-"
            lines.append(f"| {t['task_key']} | {t['kind']} | {deps} |")

    lines += ["", "## Reusable components", "",
              f"- Mapplets: {', '.join(repo.mapplets) or 'none'} -> shared module(s)",
              f"- Worklets: {', '.join(repo.worklets) or 'none'} -> inlined tasks / reusable job",
              "",
              "## Notes",
              "- Mapping parameters ($$DEPT) -> job task parameters / widgets.",
              "- Update Strategy DD_UPDATE -> Delta MERGE into the target.",
              "- `infa2dbx verify` runs the generated module against the legacy result.",
              ]
    return "\n".join(lines)
