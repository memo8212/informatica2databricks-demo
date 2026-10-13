"""CLI for infa2dbx.

    python -m infa2dbx convert --repo . --out build
    python -m infa2dbx verify --module build/module_m_load_dept_txn.py \
        --data data --legacy artifacts/output/legacy/fact_daily_customer.csv
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from infa2dbx.codegen import build_module, build_notebook, build_schema, build_workflow, is_linear
from infa2dbx.parser import load_repository
from infa2dbx.report import build_report
from infa2dbx.verify import verify


def cmd_convert(args) -> int:
    root = Path(args.repo).resolve()
    repo = load_repository(root)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    jobs = {}
    for wf in repo.workflows:
        job = build_workflow(wf)
        jobs[wf.name] = job
        (out / f"workflow_{wf.name}.json").write_text(json.dumps(job, indent=2), encoding="utf-8")

    print(f"Repository: {len(repo.mappings)} mapping(s), {len(repo.workflows)} workflow(s), "
          f"{len(repo.datasets)} source/target(s)")

    first_rows = []
    for m in repo.mappings:
        nb, rows = build_notebook(m, repo)
        (out / f"notebook_{m.name}.py").write_text(nb, encoding="utf-8")
        if is_linear(m):
            (out / f"module_{m.name}.py").write_text(build_module(m, repo), encoding="utf-8")
        job = jobs.get(repo.workflows[0].name) if repo.workflows else {}
        (out / f"conversion_report_{m.name}.md").write_text(
            build_report(repo, m, rows, job), encoding="utf-8")
        if not first_rows:
            first_rows = rows
        flag = "" if is_linear(m) else "  (non-linear: notebook only)"
        print(f"  - {m.name}: {len(rows)} transformations{flag}")

    (out / "schema.py").write_text(build_schema(repo), encoding="utf-8")
    if repo.parameters:
        (out / "parameters.json").write_text(json.dumps(repo.parameters, indent=2), encoding="utf-8")

    print("Generated into", out)
    return 0


def cmd_verify(args) -> int:
    res = verify(args.module, args.data, args.legacy, out_dir=args.out, tol=args.tol)
    print("verify:", "PASS" if res["passed"] else "FAIL")
    print(f"  rows: legacy={res['rows']} generated={res['generated_rows']} row_match={res['row_match']}")
    print(f"  checks: {res['checks']}  max_diff={res['max_diff']}")
    return 0 if res["passed"] else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="infa2dbx",
                                 description="Informatica PowerCenter -> Databricks conversion app")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("convert", help="translate PowerCenter XML -> notebooks/module/workflow")
    c.add_argument("--repo", default=".", help="repo root (contains legacy/)")
    c.add_argument("--out", default="build")
    c.set_defaults(func=cmd_convert)

    v = sub.add_parser("verify", help="run the generated module vs the legacy result")
    v.add_argument("--module", required=True)
    v.add_argument("--data", required=True, help="dir with dept_txn.csv/customer_dim.csv/fx_rates.csv")
    v.add_argument("--legacy", required=True, help="legacy fact CSV")
    v.add_argument("--out", default=None)
    v.add_argument("--tol", type=float, default=0.05)
    v.set_defaults(func=cmd_verify)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
