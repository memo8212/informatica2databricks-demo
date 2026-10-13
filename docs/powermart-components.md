# PowerCenter Components → Databricks — Deep Dive

A reference for migrating **Informatica PowerCenter** to Databricks: every major
component, what it does, and its Databricks target. Use it alongside
`converter/pc2dbx_converter.py` (which parses the PowerCenter XML export and
generates the Databricks artifacts).

---

## 1. Platform components

| PowerCenter | Role | Databricks target |
|---|---|---|
| **PowerCenter Repository Service** | stores all metadata (mappings, workflows, sessions) | **Git** (code) + **Unity Catalog** (data + lineage) + **Asset Bundles** |
| **Domain / Integration Service** | the runtime that executes sessions | **Databricks workspace + compute** (serverless/job clusters) |
| **PowerCenter Client** (Designer, Workflow Manager, Workflow Monitor) | authoring + monitoring | **notebooks/IDE + Workflows UI + job run history** |
| **Repository Manager (pmrep)** | deploy/promote objects across repos | `git` + `databricks bundle deploy` |
| **Command line (pmcmd)** | start/stop workflows, get status | `databricks jobs run-now` / `bundle run` / CLI |
| **Metadata Manager / lineage** | column-level lineage | **Unity Catalog lineage** |

---

## 2. Sources & targets

| PowerCenter source | Databricks ingestion |
|---|---|
| Relational (Oracle, DB2, Teradata, SQL Server) | **Lakeflow Connect** (managed) or JDBC read → **Bronze** |
| Flat files (fixed-width, delimited) | **Auto Loader** (`cloudFiles`) → Bronze |
| XML / JSON / Parquet | Auto Loader with a schema |
| Mainframe / VSAM / CDC (PowerExchange) | unload to Parquet → Bronze (see the sibling `mainframe2databricks-demo`) |
| MQ / Kafka / JMS | Kafka/Event Hubs source or Lakeflow Connect |
| SaaS (Salesforce, SAP, Workday) | **Lakeflow Connect** connectors |

Targets become **Delta tables** (Silver/Gold). `Update Strategy` decides
insert/update/delete → a Delta `MERGE`.

---

## 3. Transformations (the mapping logic)

| Transformation | Purpose | Databricks equivalent |
|---|---|---|
| **Source Qualifier** | read source rows, apply the source filter / join | `spark.read` (Bronze) + `filter` / `join` |
| **Expression** | derive columns with expressions | `withColumn(<port>, <translated expr>)` |
| **Filter** | keep rows matching a condition | `df.filter(<translated condition>)` |
| **Router** | split rows into groups by condition | filtered branches + `unionByName` |
| **Joiner** | join two pipelines (normal/master-outer/detail-outer) | `df.join(other, on=..., how=...)` |
| **Lookup** (connected/unconnected, cached/uncached/dynamic) | enrich from a reference table | `join` (broadcast small dims) or a Delta lookup; dynamic lookup → re-join per batch |
| **Aggregator** (sorted/unsorted) | group-by + aggregates | `groupBy(...).agg(...)` |
| **Sorter** | order rows | `orderBy` |
| **Rank** | top-N / dense rank | `Window.orderBy` + `rank()`/`row_number()` |
| **Union** | stack inputs | `unionByName` |
| **Normalizer** | rows → columns / columns → rows | `explode` / `stack` / `melt` |
| **Sequence Generator** | surrogate keys (NEXTVAL/CURRVAL) | `monotonically_increasing_id()` or **Delta identity columns** |
| **Update Strategy** | mark rows insert/update/delete (`DD_INSERT`/`DD_UPDATE`/`DD_DELETE`) | Delta **`MERGE`** |
| **Transaction Control** | commit/rollback boundaries | Delta **ACID** transactions (implicit) |
| **SQL transformation** | SQL inside the mapping | Spark SQL (`spark.sql`) |
| **Java / External Procedure** | custom code | Python/pandas UDF, or a wheel |
| **Stored Procedure** | call a DB proc | `spark.sql("CALL ...")` / connector |
| **HTTP / XML** | web/XML processing | `requests`/`xml` in Python, or a connector |
| **Mapplet** | reusable set of transformations | shared Python module / function |

### Expression function translation

`converter/infa_expr.py` translates the common function set:

| Informatica | PySpark |
|---|---|
| `IIF(c, a, b)` | `F.when(c, a).otherwise(b)` |
| `DECODE(x, k1,v1, …, default)` | nested `when/otherwise` |
| `UPPER/LOWER/TRIM/SUBSTR/INSTR/LENGTH` | `F.upper/F.lower/F.trim/F.substring/F.instr/F.length` |
| `ROUND(x,n)`, `ABS` | `F.round`, `F.abs` |
| `SUM/AVG/MIN/MAX/COUNT` | `F.sum/F.avg/F.min/F.max/F.count` |
| `NVL/COALESCE` | `F.coalesce` |
| `ISNULL(x)` | `x.isNull()` |
| `SYSDATE` | `F.current_date()` |
| `ADD_TO_DATE(d,'DD',n)` | `F.date_add(d, n)` |
| `TRUNC(d)` | `F.date_trunc('day', d)` |
| `TO_DECIMAL(x,p,s)` | `F.round(x, s)` / `cast(DecimalType(p,s))` |
| `AND / OR / NOT`, `= <> < >` | `& / \| / ~`, `== != < >` |

---

## 4. Sessions & tasks

| PowerCenter task | Databricks |
|---|---|
| **Session** (runs a mapping) | Workflow **task** (notebook / SQL / Python) |
| **Worklet** (group of tasks) | **sub-workflow** (inlined tasks) or a reusable job |
| **Command** | `python_task` / shell task |
| **Decision** | task `run_if` condition (or `if/else` in code) |
| **Timer** | Workflow **schedule** / trigger |
| **Event Wait / Event Raise** | **file-arrival** trigger or job dependency |
| **Email** | **job notification** (email/Slack/webhook) |
| **Assignment** | task parameter set / `dbutils.widgets` |
| **Control / Link** | `depends_on` edges (DAG) |

Session settings map too:

| Session setting | Databricks |
|---|---|
| Commit interval | Delta commit granularity (automatic) |
| Pre/Post SQL | pre/post task SQL (e.g. `OPTIMIZE`, `ANALYZE`) |
| Source/Target connections | Unity Catalog connections / Lakeflow Connect |
| Error handling (stop/continue) | task retries + expectations / quarantine |
| Partitioning | `repartition` / `partitionBy` / Liquid Clustering |
| Pushdown optimization | Spark predicate/projection pushdown + Delta |
| Logging / session log | Job logs + a Delta **audit** table |

---

## 5. Workflow orchestration

| PowerCenter | Databricks |
|---|---|
| Workflow | **Job** (DAG of tasks) |
| Workflow links + conditions | `depends_on` + `run_if` |
| Scheduling (`0 0 1 * * ?`) | Job schedule (cron/quartz) |
| Restart/recovery | task retries + idempotent Delta `MERGE` |
| Workflow variables (`$$VAR`) | job parameters / bundle variables |
| Parameter file (`.prm`) | bundle variables / a Delta config table |

---

## 6. Parameters & variables

- **Mapping parameters** (`$$DEPT`) → job task parameter / notebook widget.
- **Mapping variables** (persist between sessions) → Delta state table or
  `taskValues`.
- **Workflow variables** (`$$BATCH_ID`) → job parameters.
- **Parameter file** → `databricks.yml` variables (this repo also emits
  `parameters.json` for reference).

---

## 7. CDC & incremental patterns

| PowerCenter | Databricks |
|---|---|
| PowerExchange CDC | **Lakeflow Connect CDC** or Delta **Change Data Feed** |
| Incremental aggregation | streaming table / `MERGE` on a watermark |
| Slowly changing dims (SCD1/2) | Delta `MERGE` (SCD1) / effective-dated rows (SCD2) |

---

## 8. Migration gotchas (things that bite)

1. **Sequence Generator ≠ row_number.** NEXTVAL is stateful and monotonic across
   sessions; `monotonically_increasing_id` is unique but **not contiguous** and
   not stable across recomputes. For durable keys use Delta identity columns.
2. **Lookup caching.** Cached lookups assume a static dimension. On Databricks
   you re-`join` each run — cheap for small dims, but broadcast them explicitly.
3. **Update Strategy semantics.** `DD_DELETE` needs a real `MERGE … WHEN MATCHED
   THEN DELETE`; a plain overwrite silently resurrects rows.
4. **Sorted vs unsorted Aggregator.** Sorting is an optimization, not semantics —
   don't rely on input order after `groupBy`.
5. **Router groups.** Ports can differ per group; model each group as its own
   DataFrame and `unionByName(allowMissingColumns=True)` where needed.
6. **Normalizer / OCCURS.** Row explosion can blow up cardinality — watch skew.
7. **Unconnected lookups.** Called as a function inside an Expression; on
   Databricks pre-join the lookup and reference the column.
8. **Transaction Control.** PowerCenter commits in batches; Delta is ACID per
   write — remove explicit commit logic.
9. **Flat-file code pages / COMP-3.** Decode EBCDIC/COMP-3 at the edge (see the
   mainframe repo's codec) before writing Bronze.
10. **Pushdown optimization.** Rewrite as native Spark joins/filters so Catalyst
    can push down to the source.

---

## 9. What the converter automates (and what it doesn't)

`pc2dbx_converter.py` parses the PowerCenter **XML export** (`POWRMART` /
`FOLDER` / `MAPPING` / `TRANSFORMATION` / `CONNECTOR` / `WORKFLOW` / `TASK` /
`WORKFLOWLINK` / `SESSION` / `MAPPLET` / `WORKLET` / parameter file) and emits:

- a **PySpark notebook** per mapping (transformations in execution order),
- a **Workflow JSON** from the workflow tasks + links,
- **StructTypes** from sources/targets,
- a **conversion report** with the component mapping.

It covers the common transformation chain (SQ → Lookup → Expression → Filter →
Aggregator → Sequence → Update Strategy). Like the other converters in this
workspace, anything it can't translate is flagged rather than silently dropped —
review the report before production use.
