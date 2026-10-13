"""SCD2 + Delta Change Data Feed demo (mapping m_scd2_customer).

Builds a Type-2 slowly changing dimension for CUSTOMER_DIM:
  * close the current row when SEGMENT changes (DD_UPDATE)
  * insert a new current version (DD_INSERT)
then reads the Delta Change Data Feed to show the changes, and validates the
result against a pandas (PowerCenter stand-in) SCD2 implementation.

Run:  python runner/run_scd2.py
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = ROOT / "artifacts" / "output" / "scd2"

EFF_FROM_INIT = "2026-01-01"
BATCH_DATE = "2026-02-01"
COLS = ["ACCOUNT_NO", "CUSTOMER_ID", "SEGMENT", "EFFECTIVE_FROM", "EFFECTIVE_TO", "CURR_FLAG"]


def make_batch(cust: pd.DataFrame) -> pd.DataFrame:
    """Batch 2: change SEGMENT for every 5th account, add 10 new accounts."""
    b = cust.copy()
    changed = b.index % 5 == 0
    b.loc[changed, "SEGMENT"] = b.loc[changed, "SEGMENT"].map(
        {"RETAIL": "MASS", "MASS": "AFFLUENT", "AFFLUENT": "CORP",
         "SME": "CORP", "CORP": "AFFLUENT"}).fillna("MASS")
    new = pd.DataFrame({
        "ACCOUNT_NO": [f"ACCNEW{i:04d}" for i in range(1, 11)],
        "CUSTOMER_ID": [f"CNEW{i:04d}" for i in range(1, 11)],
        "SEGMENT": ["RETAIL"] * 10,
    })
    return pd.concat([b, new], ignore_index=True)


def scd2_pandas(initial: pd.DataFrame, batch: pd.DataFrame) -> pd.DataFrame:
    """PowerCenter stand-in for SCD2: close changed rows, insert new versions."""
    rows = [dict(r, EFFECTIVE_FROM=EFF_FROM_INIT, EFFECTIVE_TO=None, CURR_FLAG="Y")
            for r in initial.to_dict("records")]
    current = {r["ACCOUNT_NO"]: r for r in rows}
    for b in batch.to_dict("records"):
        cur = current.get(b["ACCOUNT_NO"])
        if cur is None:
            new = dict(b, EFFECTIVE_FROM=BATCH_DATE, EFFECTIVE_TO=None, CURR_FLAG="Y")
            rows.append(new)
            current[b["ACCOUNT_NO"]] = new
        elif cur["SEGMENT"] != b["SEGMENT"]:
            cur["CURR_FLAG"] = "N"
            cur["EFFECTIVE_TO"] = BATCH_DATE
            new = dict(b, EFFECTIVE_FROM=BATCH_DATE, EFFECTIVE_TO=None, CURR_FLAG="Y")
            rows.append(new)
            current[b["ACCOUNT_NO"]] = new
    return pd.DataFrame(rows)[COLS]


def main() -> None:
    cust = pd.read_csv(DATA / "customer_dim.csv")
    batch = make_batch(cust)
    initial = cust.copy()

    # ---- legacy (pandas) SCD2 ----
    legacy = scd2_pandas(initial, batch)
    shutil.rmtree(OUT, ignore_errors=True)
    OUT.mkdir(parents=True, exist_ok=True)
    legacy.to_csv(OUT / "legacy_dim_customer_scd2.csv", index=False)
    legacy_current = legacy[legacy["CURR_FLAG"] == "Y"].sort_values("ACCOUNT_NO").reset_index(drop=True)
    print(f"Legacy SCD2 done: {len(legacy)} versions, {len(legacy_current)} current")

    # ---- Databricks (Spark + Delta) SCD2 ----
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master("local[2]").appName("scd2_demo")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    path = str(OUT / "dim_customer_scd2")
    dim_schema = StructType([StructField(c, StringType(), True) for c in COLS])

    init_df = spark.createDataFrame(
        [(r["ACCOUNT_NO"], r["CUSTOMER_ID"], r["SEGMENT"], EFF_FROM_INIT, None, "Y")
         for r in initial.to_dict("records")],
        dim_schema)
    init_df.write.format("delta").mode("overwrite").save(path)
    spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    batch_df = spark.createDataFrame(
        [(r["ACCOUNT_NO"], r["CUSTOMER_ID"], r["SEGMENT"]) for r in batch.to_dict("records")],
        ["ACCOUNT_NO", "CUSTOMER_ID", "SEGMENT"])
    batch_df.createOrReplaceTempView("batch")

    # STEP 1 (DD_UPDATE): close the current row when the tracked attribute changed
    spark.sql(f"""
        MERGE INTO delta.`{path}` AS t
        USING batch AS s
        ON t.ACCOUNT_NO = s.ACCOUNT_NO AND t.CURR_FLAG = 'Y' AND t.SEGMENT <> s.SEGMENT
        WHEN MATCHED THEN UPDATE SET t.CURR_FLAG = 'N', t.EFFECTIVE_TO = '{BATCH_DATE}'
    """)
    # STEP 2 (DD_INSERT): insert a new current version where none exists
    spark.sql(f"""
        INSERT INTO delta.`{path}`
        SELECT s.ACCOUNT_NO, s.CUSTOMER_ID, s.SEGMENT, '{BATCH_DATE}', NULL, 'Y'
        FROM   batch s
        LEFT JOIN delta.`{path}` t ON t.ACCOUNT_NO = s.ACCOUNT_NO AND t.CURR_FLAG = 'Y'
        WHERE  t.ACCOUNT_NO IS NULL
    """)

    dim = spark.read.format("delta").load(path).toPandas()
    dbx_current = dim[dim["CURR_FLAG"] == "Y"].sort_values("ACCOUNT_NO").reset_index(drop=True)

    # ---- Change Data Feed ----
    cdf = (spark.read.format("delta")
           .option("readChangeFeed", "true")
           .option("startingVersion", 1)
           .load(path)
           .select("ACCOUNT_NO", "SEGMENT", "CURR_FLAG", "_change_type")
           .toPandas())
    cdf.to_csv(OUT / "change_data_feed.csv", index=False)
    print(f"CDF rows: {len(cdf)}  {dict(cdf['_change_type'].value_counts())}")

    # ---- validation ----
    merged = legacy_current.merge(dbx_current, on="ACCOUNT_NO", suffixes=("_INFA", "_DBX"),
                                  how="outer", indicator=True)
    row_match = bool((merged["_merge"] == "both").all())
    seg_ok = bool((merged["SEGMENT_INFA"] == merged["SEGMENT_DBX"]).all()) if row_match else False
    hist_ok = len(legacy) == len(dim)
    passed = row_match and seg_ok and hist_ok

    report = f"""# Validation Report — m_scd2_customer (SCD2 + Change Data Feed)

## Result: {"✅ PASS — SCD2 matches (current rows + history)" if passed else "❌ FAIL"}

| Metric | Value |
|---|---|
| Accounts (initial) | {len(initial)} |
| Batch rows (changes + new) | {len(batch)} |
| Total versions (legacy / Databricks) | {len(legacy)} / {len(dim)} |
| Current rows match | {row_match} |
| Current SEGMENT match | {seg_ok} |
| History row count match | {hist_ok} |
| CDF change rows | {len(cdf)} |

## Change Data Feed (sample)
```
{cdf.head(10).to_string(index=False)}
```
"""
    (OUT / "validation_report.md").write_text(report, encoding="utf-8")
    print("Validation done -> artifacts/output/scd2/validation_report.md")
    print(report.split("## Change Data Feed")[0])
    spark.stop()
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
