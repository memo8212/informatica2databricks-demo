"""Run a generated module against the legacy (PowerCenter) result."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

DEFAULT_KEYS = ["CUSTOMER_ID", "SEGMENT", "DEPT", "TXN_DATE"]
DEFAULT_MEASURES = ["AMOUNT_BASE"]


def _load_module(path: str | Path):
    spec = importlib.util.spec_from_file_location("generated_module", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)          # type: ignore[union-attr]
    return mod


def _spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("infa2dbx-verify")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false").getOrCreate())


def verify(module_path: str | Path, data_dir: str | Path, legacy_csv: str | Path,
           out_dir: str | Path | None = None, keys: list | None = None,
           measures: list | None = None, tol: float = 0.05) -> dict:
    keys = keys or DEFAULT_KEYS
    measures = measures or DEFAULT_MEASURES
    data_dir = Path(data_dir)
    bridge = Path(out_dir) if out_dir else data_dir.parent / "verify_bridge"
    bridge.mkdir(parents=True, exist_ok=True)

    # build the Parquet bridge from the sample extracts
    for name in ("dept_txn", "customer_dim", "fx_rates"):
        src = data_dir / f"{name}.csv"
        if src.exists():
            (bridge / name).mkdir(parents=True, exist_ok=True)
            pd.read_csv(src).to_parquet(bridge / name / "part-00000.parquet", index=False)

    spark = _spark()
    mod = _load_module(module_path)
    result = mod.run(spark, str(bridge)).toPandas()
    spark.stop()

    legacy = pd.read_csv(legacy_csv)
    merged = legacy.merge(result, on=keys, suffixes=("_INFA", "_DBX"), how="outer", indicator=True)
    row_match = bool((merged["_merge"] == "both").all())
    checks = {}
    for m in measures:
        checks[m] = bool(((merged[f"{m}_INFA"] - merged[f"{m}_DBX"]).abs() <= tol).all()) if row_match else False
    max_diff = float((merged[f"{measures[0]}_INFA"] - merged[f"{measures[0]}_DBX"]).abs().max()) if row_match else float("nan")
    return {"passed": row_match and all(checks.values()), "rows": int(len(legacy)),
            "generated_rows": int(len(result)), "row_match": row_match,
            "checks": checks, "max_diff": max_diff}
