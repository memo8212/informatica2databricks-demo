# Conversion Report — m_load_dept_txn / WF_DAILY_BANK_FEED

Informatica PowerCenter -> Databricks. Automatic mapping summary.

## Component mapping

| PowerCenter component | Databricks target |
|---|---|
| Source Qualifier | read Bronze table (+ WHERE) |
| Lookup Procedure | join to lookup table |
| Expression | withColumn(translated expression) |
| Filter | filter(translated condition) |
| Router | multiple filtered branches (union on read) |
| Joiner | join (broadcast for small dims) |
| Aggregator | groupBy(...).agg(...) |
| Sorter | orderBy |
| Rank | Window.orderBy + rank() |
| Union | unionByName |
| Normalizer | explode / stack |
| Sequence Generator | monotonically_increasing_id / Delta identity |
| Update Strategy (DD_*) | Delta MERGE (UPDATE/INSERT/DELETE) |
| Transaction Control | Delta ACID transaction |
| Session | Databricks Workflow task (notebook) |
| Workflow | Databricks Workflow (job) |
| Worklet | sub-workflow (inlined tasks) / reusable job |
| Mapplet | reusable function / shared module |
| Decision task | task run_if condition |
| Command task | python/shell task |
| Email task | job notification |
| Timer | schedule / trigger |
| Parameter file (.prm) | job parameters / bundle variables |
| Connection (relational/file) | Unity Catalog + Auto Loader / Lakeflow Connect |
| Repository (pmrep) | Git + Unity Catalog |
| pmcmd | databricks CLI / jobs run-now |

## Mapping transformations (in execution order)

| # | Transformation | Type | Generated |
|---|---|---|---|
| 1 | SQ_DEPT_TXN | Source Qualifier | read bronze_dept_txn + dept filter |
| 2 | LKP_CUSTOMER | Lookup Procedure | join customer_dim on ACCOUNT_NO |
| 3 | LKP_FX | Lookup Procedure | join fx_rates on CCY |
| 4 | EXP_NORMALIZE | Expression | DEPT; AMOUNT_BASE; IS_REV |
| 5 | FILT_ACTIVE | Filter | IS_REV = 'N' AND AMOUNT_BASE <> 0 |
| 6 | AGG_DAILY | Aggregator | groupBy ['CUSTOMER_ID', 'SEGMENT', 'DEPT', 'TXN_DATE'] agg ['AMOUNT_BASE'] |
| 7 | SEQ_TXN_KEY | Sequence Generator | withColumn TXN_KEY |
| 8 | UPD_STRAT | Update Strategy | MERGE (DD_UPDATE) |

## Workflow tasks

| Task | PowerCenter type | Databricks kind | Depends on |
|---|---|---|---|
| wklt_dept_ingest | Worklet | sub-workflow | - |
| dec_rowcount | Decision | conditional | wklt_dept_ingest |
| cmd_refresh_gold | Command | python_task | dec_rowcount |
| s_gold_summary | Session | notebook | cmd_refresh_gold |
| email_notify | Email | notification | s_gold_summary |

## Reusable components

- Mapplets: mplt_normalize_txn -> shared PySpark module(s)
- Worklets: wklt_dept_ingest -> inlined tasks / reusable job

## Notes
- The mapping is parameterized by `$$DEPT`; each session supplies a value from `bank_params.prm`. Migrated to a Databricks job parameter / widget.
- Update Strategy `DD_UPDATE` becomes a Delta `MERGE` into the target.
- Pre/Post SQL becomes a post-load `OPTIMIZE`/`ANALYZE` task.
- `pmcmd startworkflow` becomes a Databricks job trigger / `run_job_task`.