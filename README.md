# Informatica PowerCenter → Databricks Migration POC

End-to-end demo: a bank's **nightly department-feed consolidation** built in
**PowerCenter** (mappings, sessions, worklets, mapplets, workflow, parameter file,
`pmcmd`) migrated to **Databricks** (Workflows + PySpark + Delta), with a
PowerCenter-vs-Databricks validation report.

Same shape as the `sas2databricks-demo` and `mainframe2databricks-demo`, but for
**Informatica PowerCenter**.

## The legacy application (a bank with many department feeds)

```
WF_DAILY_BANK_FEED  (schedule 0 0 1 * * ?)
  start → wklt_dept_ingest (worklet)
            s_dept_cards    ┐
            s_dept_payments │  mapping m_load_dept_txn  (parameterized $$DEPT)
            s_dept_loans    │  SQ_DEPT_TXN → LKP_CUSTOMER → LKP_FX → EXP_NORMALIZE
            s_dept_deposits │  → FILT_ACTIVE → AGG_DAILY → SEQ_TXN_KEY → UPD_STRAT
            s_dept_wealth   ┘  → target FACT_DAILY_CUSTOMER
        → dec_rowcount (decision) → cmd_refresh_gold (pmcmd) → s_gold_summary → email_notify
```

Five departments (CARDS, PAYMENTS, LOANS, DEPOSITS, WEALTH) each drop a feed; the
**same mapping** processes each via the mapping parameter `$$DEPT`. Lookups enrich
with the customer segment and FX rates; the result is a daily customer×dept fact.

## Component mapping (PowerCenter → Databricks)

| PowerCenter | Databricks |
|---|---|
| Workflow | Databricks Workflow (job) |
| Worklet | sub-workflow / inlined tasks |
| Session | job task (notebook) |
| Mapping | PySpark notebook |
| Mapplet | reusable function / shared module |
| Source Qualifier | read Bronze (+ filter) |
| Lookup Procedure | join (broadcast small dims) |
| Expression | `withColumn(translated expr)` |
| Filter | `filter(translated condition)` |
| Aggregator | `groupBy(...).agg(...)` |
| Sequence Generator | `monotonically_increasing_id` / Delta identity |
| Update Strategy (DD_UPDATE) | Delta `MERGE` |
| Decision / Command / Email task | `run_if` / python task / notification |
| Parameter file (`.prm`) | job parameters / bundle variables |
| Repository (pmrep) | Git + Unity Catalog |
| `pmcmd` | `databricks jobs run-now` / `bundle run` |

Full reference: **`docs/powermart-components.md`** (every component + gotchas).

## Structure

```
legacy/                 PowerCenter repository export + config
  workflows/WF_DAILY_BANK_FEED.xml
  mappings/m_load_dept_txn.xml
  mapplets/mplt_normalize_txn.xml   (mapplet + worklet)
  sessions/session_config.xml
  sources/sources_targets.xml
  parameters/bank_params.prm
  scripts/run_workflow.sh           (pmcmd)
  oracle/DDL.sql
converter/
  infa_expr.py                      Informatica expression -> PySpark
  pc2dbx_converter.py               XML export -> notebook + workflow + report
runner/run_demo.py                  legacy -> parquet bridge -> Databricks -> validation
data/generate_bank_feeds.py         synthetic department feeds + lookups
notebooks/                          migrated notebooks (Asset Bundle)
resources/                          bundle job + uc_setup.sql
databricks.yml                      Databricks Asset Bundle root
docs/powermart-components.md        component deep dive
tests/                              pytest
artifacts/                          generated outputs
```

## Run

```bash
pip install pyspark delta-spark pyarrow pandas pytest

python data/generate_bank_feeds.py         # synthetic dept feeds + lookups
python converter/pc2dbx_converter.py       # PowerCenter XML -> Databricks artifacts
python runner/run_demo.py                  # both sides + validation report
pytest -q tests/test_converter.py          # converter tests
```

## Outputs (in `artifacts/`)

- `generated/conversion_report.md` — component + transformation mapping
- `generated/notebook_m_load_dept_txn.py` — mapping as a PySpark notebook
- `generated/databricks_workflow.json` — workflow tasks + dependencies
- `generated/schema.py` — sources/targets as StructTypes
- `generated/parameters.json` — parameter file parsed
- `output/validation_report.md` — PowerCenter vs Databricks parity

## Deploy to Databricks (Asset Bundle)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run wf_daily_bank_feed -t dev
```

`resources/bank_job.yml` reproduces the workflow: `ingest → 5 dept sessions →
gold_summary`, with an email notification on failure.

## What the demo proves

1. **Structure migrates mechanically** — workflow/worklet/session/task → job/task DAG.
2. **Mapping logic migrates** — SQ/Lookup/Expression/Filter/Aggregator → PySpark,
   with Informatica functions translated (`IIF`, `ROUND`, `UPPER`, `DECODE`, …).
3. **Parameterization survives** — `$$DEPT` from the parameter file → task widget.
4. **Upserts become MERGEs** — `Update Strategy DD_UPDATE` → Delta `MERGE`.
5. **Parity is provable** — legacy vs Databricks compared row-by-row (10,545 fact
   rows, max amount diff `0.0`).

## Notes / scope

- The converter is a **POC**: it parses the common PowerCenter XML constructs
  (`MAPPING`/`TRANSFORMATION`/`CONNECTOR`/`WORKFLOW`/`TASK`/`WORKFLOWLINK`/
  `SESSION`/`MAPPLET`/`WORKLET` + parameter file) to demonstrate mapping, not a
  full PowerCenter importer. Review the report before production use.
- The runner re-implements the mapping in **pandas** (legacy stand-in) and
  **PySpark** (target) so both sides run locally; money is handled in integer
  cents for exact parity.

## License

MIT — see `LICENSE`.
