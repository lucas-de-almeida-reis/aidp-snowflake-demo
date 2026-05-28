# aidp-snowflake-demo

End-to-end **medallion pipeline** for Rappi running on OCI:

- **Bronze** lives in **Snowflake** — synthetic order facts under `RAPPI_DEV.BRONZE.ORDER_DIMENSIONS` (dev) and `RAPPI_PROD.BRONZE.ORDER_DIMENSIONS` (prod) on the same account.
- **Silver** lives in **AIDP** as **Delta** in the standard catalog.
- **Gold** lives in **ADW / ALH** (Autonomous), reached from AIDP through an **external catalog** mount — code-wise it's just another catalog name.
- **Airflow** orchestrates the two AIDP jobs (silver, then gold) — one DAG per env (`rappi_medallion_dev`, `rappi_medallion_prod`).

```
┌─────────────────┐   spark-snowflake   ┌─────────────────────┐   delta saveAsTable   ┌──────────────────────┐
│   Snowflake     │  ──── 64 MB chunks ─►│   AIDP (Spark)      │ ─────────────────────►│  AIDP std catalog    │
│   bronze        │   (projection +      │ 01_bronze_to_silver │  partitionBy          │  silver.orders       │
│   ORDER_DIM_*   │    predicate         │     .ipynb          │  (country, day)       │  (Delta)             │
└─────────────────┘    pushdown)         └─────────────────────┘                       └──────────┬───────────┘
                                                                                                  │
                                                                                                  ▼
                                                                          ┌─────────────────────────┐
                                                                          │   AIDP (Spark)          │
                                                                          │   02_silver_to_gold     │
                                                                          │       .ipynb            │
                                                                          └─────────────┬───────────┘
                                                                                        │ saveAsTable
                                                                                        │ via external catalog
                                                                                        ▼
                                                                          ┌──────────────────────────┐
                                                                          │   ADW / ALH              │
                                                                          │   gold.vertical_         │
                                                                          │   performance            │
                                                                          └──────────────────────────┘

                  orchestrated by:    Airflow (full_pipeline.py) — chained AIDP jobRuns
```

---

## Repository layout

```
aidp-snowflake-demo/
├── README.md
└── assets/
    ├── snowflake/                          # Bronze layer (Snowflake)
    │   ├── 0-initial-check.sql             # bootstrap: creates BOTH RAPPI_DEV + RAPPI_PROD
    │   ├── dev/                            # run after bootstrap
    │   │   ├── 1-create-table.sql          # CREATE OR REPLACE in RAPPI_DEV.BRONZE
    │   │   ├── 2-insert-data.sql           # 100k synthetic rows
    │   │   └── 3-verify-table.sql          # sanity counts
    │   └── prod/                           # mirror of dev/, different DB + 10M rows
    │       ├── 1-create-table.sql
    │       ├── 2-insert-data.sql
    │       └── 3-verify-table.sql
    ├── aidp/
    │   ├── jars/                           # uploaded to the AIDP cluster classpath
    │   │   └── (download from Maven Central — see step 2.1 below)
    │   └── notebooks/
    │       ├── 00_connector_smoke_test.ipynb   # one-off: confirm the connector works
    │       ├── 01_bronze_to_silver.ipynb       # Snowflake → silver.orders (Delta)
    │       └── 02_silver_to_gold.ipynb         # silver.orders → gold.vertical_performance (ADW)
    └── airflow/
        ├── full_pipeline.py                # the medallion DAG
        ├── Dockerfile                      # single-container Airflow (SQLite + SequentialExecutor)
        ├── entrypoint.sh                   # init DB → ensure admin user → scheduler + webserver
        └── deploy.py                       # build + push to OCIR + deploy as OCI Container Instance
```

---

## How to set up the environment

### 1. Snowflake — bronze

Run against a worksheet attached to **COMPUTE_WH**:

1. `0-initial-check.sql` — bootstrap. Idempotent. Hardens warehouse auto-suspend and creates **both** `RAPPI_DEV.BRONZE` and `RAPPI_PROD.BRONZE` in one shot.
2. **For dev:** run [`dev/1-create-table.sql`](assets/snowflake/dev/1-create-table.sql) → [`dev/2-insert-data.sql`](assets/snowflake/dev/2-insert-data.sql) (seeds 100k rows) → [`dev/3-verify-table.sql`](assets/snowflake/dev/3-verify-table.sql).
3. **For prod:** run [`prod/1-create-table.sql`](assets/snowflake/prod/1-create-table.sql) → [`prod/2-insert-data.sql`](assets/snowflake/prod/2-insert-data.sql) (seeds 10M rows) → [`prod/3-verify-table.sql`](assets/snowflake/prod/3-verify-table.sql).

Schema (`BRONZE`) and table (`ORDER_DIMENSIONS`) names are identical inside each DB — only the database name differs, so the AIDP notebooks switch envs by changing `sfDatabase` in `config.yaml` and nothing else.

Snowflake user/role for the AIDP job needs `USAGE` on the warehouse + database + schema and `SELECT` on the table.

### 2. AIDP workspace — silver + gold

1. **Download the Snowflake Spark connector JARs** from Maven Central into `assets/aidp/jars/` (they are not checked in — see [.gitignore](.gitignore)):
   - [spark-snowflake_2.12-3.1.1.jar](https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.1.1/spark-snowflake_2.12-3.1.1.jar)
   - [snowflake-jdbc-3.19.0.jar](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.19.0/snowflake-jdbc-3.19.0.jar)

   Then upload both to your AIDP workspace and add them to the cluster classpath (Workspace → Cluster → Libraries → upload).
2. **Upload both notebooks** under `assets/aidp/notebooks/` to AIDP (Workspace → Notebooks → upload).
3. **Register each notebook as an AIDP Job** (Workspace → Jobs → Create) — one job per notebook:
   - `silver-from-snowflake` → `01_bronze_to_silver.ipynb`
   - `gold-to-adw`           → `02_silver_to_gold.ipynb`
   Note the resulting **job keys** — Airflow needs them.
4. **Set notebook environment variables** in each job's runtime config (or as cluster-level secrets):
   - Snowflake source — `SNOW_URL`, `SNOW_USER`, `SNOW_PASSWORD`, `SNOW_DATABASE=RAPPI_SANDBOX`, `SNOW_SCHEMA=SYNTH`, `SNOW_WAREHOUSE=COMPUTE_WH`, `SNOW_TABLE=ORDER_DIMENSIONS_SYNTH`
   - Silver target — `SILVER_CATALOG`, `SILVER_SCHEMA=silver`, `SILVER_TABLE=orders`
   - Gold target — `GOLD_CATALOG` (the name of the external-catalog mount), `GOLD_SCHEMA=gold`, `GOLD_TABLE=vertical_performance`
   - Optional — `INCREMENTAL_FROM=YYYY-MM-DD` to switch the silver notebook to incremental mode (writes only that partition slice via `replaceWhere`).

### 3. ADW / ALH — gold

The gold notebook writes to ADW through the **external catalog** mount configured at the AIDP workspace level. Once that mount exists (admin-side, via the AIDP console — provide the wallet there, once), the notebook never touches a JDBC URL or wallet path: `saveAsTable("<catalog>.gold.vertical_performance")` is all the code does. The catalog name you assign at mount-time becomes the value of `GOLD_CATALOG`.

After the first gold run you can verify in ADW with:
```sql
SELECT * FROM GOLD.VERTICAL_PERFORMANCE
ORDER BY ORDER_MONTH DESC, GMV DESC
FETCH FIRST 20 ROWS ONLY;
```

### 4. Airflow — orchestration

`full_pipeline.py` exposes **two DAGs** built from a factory — one per env, each triggering its env's pair of AIDP jobs in sequence. Required env vars (set via `config-deploy.yaml` → baked into the container by `deploy.py`):

| Variable                  | What it is |
|---|---|
| `TENANCY_ID`, `USER_ID`, `FINGERPRINT`, `PRIVATE_KEY` | OCI API-key auth for signing AIDP REST calls. Auto-extracted from `~/.oci/config` by `deploy.py`. |
| `AIDP_REGION`             | e.g. `sa-saopaulo-1`. |
| `AIDP_ID`                 | OCID of the AIDP dataLake. |
| `AIDP_WORKSPACE_ID`       | OCID of the workspace. |
| `AIDP_DEV_BRONZE_JOB_KEY` / `AIDP_DEV_GOLD_JOB_KEY`   | Dev AIDP job keys for `01_bronze_to_silver.ipynb` and `02_silver_to_gold.ipynb`. |
| `AIDP_PROD_BRONZE_JOB_KEY` / `AIDP_PROD_GOLD_JOB_KEY` | Prod AIDP job keys (same notebooks, separate AIDP jobs per env). |

Pick a DAG in the Airflow UI (`rappi_medallion_dev` or `rappi_medallion_prod`) → Trigger DAG. The task graph for each is:

```
trigger_bronze_to_silver → wait_bronze_to_silver → trigger_silver_to_gold → wait_silver_to_gold
```

### 4a. Deploying Airflow as an OCI Container Instance

`deploy.py` builds the Airflow image from `Dockerfile`, pushes it to OCIR, sets up VCN/subnet/security-list, creates (or verifies) a Dynamic Group + IAM policy granting `use ai-data-platforms`, and launches the container.

The deployed container authenticates to AIDP via **resource principal** — no `PRIVATE_KEY` env var, no secret to rotate. The DAG's `_aidp_signer()` helper detects `OCI_RESOURCE_PRINCIPAL_VERSION` (which OCI Container Instances sets automatically) and uses `oci.auth.signers.get_resource_principals_signer()`; locally, it falls back to `~/.oci/config` so the same DAG file works on your laptop without changes.

```bash
cd assets/airflow

# fully interactive — auto-discovers from ~/.oci/config; prompts for AIDP IDs + job keys
python3 deploy.py

# non-interactive (uses env vars + config-deploy.yaml + defaults)
AIDP_REGION=sa-saopaulo-1 \
AIDP_ID=ocid1.dataLake.oc1... \
AIDP_WORKSPACE_ID=ocid1.workspace.oc1... \
AIDP_SILVER_JOB_KEY=... \
AIDP_GOLD_JOB_KEY=... \
python3 deploy.py --yes --create-iam
```

What you get back:

```
Airflow is running.

Public IP:  XXX.XXX.XXX.XXX
Access at:  http://XXX.XXX.XXX.XXX:8080

  user: admin
  pass: admin     (override via AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD)
```

If a `jobRuns` call later returns 403, add the Dynamic Group OCID (printed at the end of the deploy) as a member of the AIDP workspace under **Workspace → Roles**. IAM alone covers `use ai-data-platforms`, but workspace-level membership is occasionally required for specific operations.

---

## What to expect when it runs

| Stage              | Rough time on 10M rows  | Output                                              |
|---|---|---|
| Snowflake → silver | 3–6 min                 | Delta table partitioned by `(country_code, order_date)` |
| Silver → gold      | 30–90 s                 | ~10K rows in `gold.vertical_performance` on ADW |

Total wall-clock for the medallion legs typically lands under **8 minutes** for the synthetic 10M-row dataset on a modest AIDP cluster.

---

## Spark connector deep-dive — why this scales

The bronze → silver step uses the **official Snowflake Spark connector** (`net.snowflake.spark.snowflake`, version 2.12-3.1.1) with `format("snowflake")`. The notebook is intentionally simple: ~20 lines of read code, ~20 lines of write. Most of the work happens inside the connector.

### How the data actually moves

```
   Spark driver                      Snowflake                   Spark executors
        │                                 │                            │
        │  send pushdown plan             │                            │
        ├────────────────────────────────►│                            │
        │  (SELECT + WHERE)               │                            │
        │                                 │  execute pruned query,     │
        │                                 │  spool results to an       │
        │                                 │  internal stage as Arrow   │
        │                                 │                            │
        │  receive pre-signed URLs        │                            │
        │◄────────────────────────────────┤                            │
        │  for each result chunk          │                            │
        │                                 │                            │
        │  schedule one Spark partition   │                            │
        │  per chunk                      ──── each executor pulls ───►│
        │                                                  its own     │
        │                                          arrow chunk(s)      │
```

The connector is **pushdown-aware**: when you use `.option("query", "SELECT col1, col2 FROM t WHERE day >= ...")` it sends the projection and predicate to Snowflake's query engine. Combined with the bronze table's `CLUSTER BY (TO_DATE(ORDER_CREATED_AT), COUNTRY_CODE)`, Snowflake prunes micro-partitions *before* any bytes leave the warehouse.

### Why it's scalable

1. **Parallelism is automatic.** Spark partitions are sized by Snowflake's result chunks, not by table size or arbitrary hash. The connector reads each chunk in parallel via pre-signed URLs against Snowflake's internal stage — no single-threaded JDBC bottleneck.
2. **Arrow over the wire.** Result chunks ship as Apache Arrow batches, not row-by-row JDBC. That's a ~5–10× wire/CPU win versus the generic `format("jdbc")` driver.
3. **Pushdown of projection, filters, and many aggregates.** With Catalyst's optimizer + the connector's pushdown rules, `df.filter(...).select(...).groupBy(...).count()` often executes mostly inside Snowflake — Spark just receives the small aggregate, not the raw rows.
4. **One knob for tuning.** `partition_size_in_mb` (default 8) lets you trade more, smaller partitions (better skew handling) vs. fewer, larger partitions (less scheduling overhead). We use 64 MB here for the 10M-row pull.
5. **Query tagging.** Setting `.option("application", "aidp-demo-bronze-to-silver")` tags every query the connector fires in Snowflake's `QUERY_HISTORY`. Cost attribution and forensics become trivial — you can filter by application and see exactly what AIDP did, when, on which warehouse.

### Advantages over alternatives

| Alternative                              | Why we don't use it                                            |
|---|---|
| Snowflake → S3 → Spark `read.parquet`   | Two hops, two storage costs, no live freshness, scheduling toil. |
| Snowflake → AIDP via generic `format("jdbc")` | No pushdown, row-by-row JDBC fetch, no parallelism without manual `partitionColumn` setup, no Arrow. |
| Snowflake COPY INTO external stage      | Operationally fine for one-off exports but every change needs Snowflake-side DDL and stage cleanup. |
| Snowpark Container Services             | Pulls compute into Snowflake — opposite direction of "land data in OCI." |

### Concerns to keep in mind

- **Cluster classpath, not notebook-local.** The two JARs (`spark-snowflake_2.12-3.1.1.jar` + `snowflake-jdbc-3.19.0.jar`) must live on every Spark executor. Don't try `--packages` at notebook init — it works in interactive runs but flakes out under AIDP-job scheduling.
- **Connector ↔ Spark version skew.** `spark-snowflake_2.12-3.1.1` is the right artifact for AIDP's Spark 3.x / Scala 2.12 runtime. If you ever upgrade Spark to 3.5+ or move to Scala 2.13, you'll need a matching connector build — the version naming is unforgiving.
- **Pushdown can be defeated by Python UDFs.** Once you call a Python UDF in a transformation chain, the optimizer stops being able to push the surrounding work down. Keep transformations in DataFrame DSL / SQL until after the data lands in silver.
- **Credentials are passwords, today.** This demo uses `sfUser` / `sfPassword`. For production, rotate these via a vault / OCI Secrets Manager and consider Snowflake's key-pair auth, which the connector supports via `pem_private_key` instead of `sfPassword`.
- **Warehouse autosuspend matters.** The bronze warehouse auto-suspends after 60 s. A cold AIDP job pays the warehouse resume tax (~5–10 s) once per run — fine for batch, painful if you were trying to drive an interactive request-response.
- **Result chunks die on driver restart.** A failed Spark stage that needs to re-read a chunk after the Snowflake stage URL has expired will require a fresh query against Snowflake. Long-running jobs with aggressive task retries should be configured with a shorter `partition_size_in_mb` (more chunks = each one cheaper to redo).

---

## Re-running, idempotency, and incremental loads

- **Full refresh (default)** — Silver and gold both write with `mode("overwrite")`. Safe to re-run; replaces the whole table.
- **Incremental silver** — Set `INCREMENTAL_FROM=2025-01-01` on the silver job. The notebook pushes the date filter to Snowflake (pruned read) and writes with Delta's `replaceWhere` so only the affected `order_date` partitions are atomically swapped.
- **Gold is always derived from silver** — Re-running gold after a silver refresh is the recommended pattern; the gold mart is small enough that full recompute is cheaper than reasoning about incremental aggregates.
