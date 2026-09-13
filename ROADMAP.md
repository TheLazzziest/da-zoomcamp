# Roadmap — da-zoomcamp → Data Engineering interview readiness

Hands-on extension plan for the `da-zoomcamp` repo. Goal: cover the surface an
EPAM-style Data Engineering loop actually probes — data modeling, orchestration,
cloud, streaming, distributed compute, IaC — using one continuous project rather
than disconnected tutorials.

The plan is revised against the current repo state and the constraints set by the
owner (few intense days, ClickHouse as system of record, DuckDB compute-only, GCP
as the cloud, Airflow dockerized, Kafka + Flink together, Terraform last).

---

## 1. Current state (audit)

**Ingestion / core**

- `src/main.py` — Typer CLI; DI via `dependency-injector`; `dlt.pipeline` runner.
- `src/core/` — `settings.py` (pydantic-settings, `da_` prefix, nested `__`),
  `containers.py`, `enums.py` (`Destination`: DUCKDB, CH, S3, BIGQUERY),
  `loguru.py`.
- `src/sources/nyc/` — dlt source `factory()`, per-category pydantic schemas
  (yellow/green/fhv/fhvhv), schema discovery via subclass registry, ClickHouse
  adapter (`merge_tree` / `replicated_merge_tree`), parallelized source, HTTP
  HEAD probe then parquet read through DuckDB → Arrow batches.
- `pyproject.toml` — Python 3.13, `dlt[clickhouse,duckdb,filesystem,parquet]`,
  `polars`, `pydantic(-settings,-extra-types)`, `httpx`, `orjson`, `loguru`,
  `typer`, `dependency-injector`.

**Storage / infra**

- `docker-compose.yaml` — ClickHouse 25.3 (HTTP 8123, TCP 9000, db `sandbox`),
  MinIO (S3-compatible, ports 9010/9011), bucket bootstrap job. **MinIO is
  replaced by GCS** (see Phase 3) — one object store for both environments.
- Trino config already present: `.trino/da.properties` (Iceberg, JDBC catalog →
  Postgres metastore, warehouse `s3://warehouse`, native S3 → MinIO) and
  `etc/catalog/{bigquery,clickhouse}.properties`. Trino is **local-only**; in
  the cloud BigQuery replaces it. The Iceberg warehouse moves from
  `s3://warehouse` (MinIO) to `gs://<bucket>/warehouse` (GCS).
- `.env.example` — GCP project/key; ClickHouse is local, so CH Cloud vars drop.

**Empty scaffolds (intent already expressed)**

- `src/readers/`, `src/pipelines/`, `src/transformers/` — `__init__.py` only.
- `tests/` — empty `conftest.py`.

**Absent**

- Modeling layer (no dbt), data-quality layer (no GE), orchestration (no
  Airflow), CI, streaming, distributed compute, IaC, BI.

**Constraints / principles**

- **Two environments, one object store.** Local = dev, cloud = prod. **GCS is the
  object store for both** — MinIO is dropped.
- **ClickHouse = local system of record.** Runs in Docker, local only; never the
  deployed store. DuckDB is in-memory compute only; never a persisted target.
- **BigQuery = cloud warehouse + query engine.** In the cloud it *replaces* Trino
  — BigQuery has its own engine, so no Trino is deployed there.
- **Trino = local query/federation layer** over ClickHouse + Iceberg-on-GCS.
  Local Trino ≈ cloud BigQuery: both are the query engine over the data.
- **Medallion layering**: bronze (raw, dlt) → silver (cleaned/conformed) → gold
  (marts), same models in both environments via dbt targets.
- Every phase must ship a runnable slice and leave an interview artifact
  (diagram box, README decision, STAR story).

---

## 2. Phase 0 — Tooling baseline: `prek`

Scope: hooks only for now. No test framework polish yet.

`prek` (Rust, pre-commit-config compatible) as the hook runner.

- `.pre-commit-config.yaml` consumed by `prek install`.
- **pre-commit stage**
  - `ruff` + `ruff --fix` (lint)
  - `ruff-format` (format)
  - `mypy` (types; `loguru-mypy` already a dev dep)
  - `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-toml`,
    `check-added-large-files`
  - `gitleaks` or `detect-secrets` (secrets guard — `.dlt/secrets.toml` exists)
- **pre-push stage** (grows with later phases)
  - `pytest` (fast subset)
  - `dbt test` (Phase 1)
  - Great Expectations checkpoint (Phase 1)
- Mirror the same checks in GitHub Actions so hooks and CI never drift.

**Done when:** `prek install` runs; a commit and a push both trigger the right
hook set; CI runs the same config.

**Interview topic:** local quality gates, CI parity.

---

## 3. Phase 1 — Modeling + quality: dbt + Great Expectations

Scope: the modeling and data-quality core, plus a dataset expansion so the model
is worth building.

### 3.1 Dataset expansion (needed for a real model)

Current data is a single fact (NYC TLC trips) with no dimensions — not enough for
staging/intermediate/marts, SCD, or multi-fact joins. Add sources that introduce
**dimensions, a second fact, text, geo, and time-series**:

| Source | Role | Grain | Why it's here | Access |
|---|---|---|---|---|
| NYC TLC trips (existing) | fact | trip | base fact | public parquet over HTTP |
| TLC **taxi zone lookup** | dim (seed) | zone | `PULocationID`/`DOLocationID` → borough/zone; classic dim + dbt seed | static CSV |
| NYC **311 Service Requests** | fact #2 | request | text (`complaint_type`, `descriptor`), status changes → **SCD2 snapshot** | Socrata API (free, no key) |
| **Weather** (Open-Meteo historical / NOAA GHCN) | dim_date enrichment | day | join trips by date; seasonality/impact marts | Open-Meteo free, no key |
| **Citi Bike** trips | fact #2 | trip | station dims, geo, second mobility fact | public monthly S3 |
| **ECB exchange rates** | dim | day×currency | currency normalization; small dim | ECB free CSV/XML |

This yields: a star schema (facts + conformed dims), a true SCD2 case (311
status), text analytics, geospatial dims, and a date dimension — enough for
`staging → intermediate → marts`.

### 3.2 dbt

- Adapters: `dbt-clickhouse` (**dev target** = local ClickHouse) and
  `dbt-bigquery` (**prod target** = cloud BigQuery). Same models, two targets.
  DuckDB stays a dlt compute engine, not a dbt target.
- Layers:
  - `staging/stg_*` — 1:1 with bronze, typing/renaming, no joins
  - `intermediate/int_*` — conformed joins, dedupe, business logic
  - `marts/` — `dim_*` (zone, date, vendor, currency, station) and `fct_*`
    (trips, 311 requests, citibike trips)
- dbt features to exercise: **seeds** (zone lookup), **snapshots** (311 SCD2),
  **generic tests** (`unique`, `not_null`, `relationships`, `accepted_values`),
  **singular tests**, **macros**, `dbt docs` + lineage graph.
- Materializations: view / table / incremental (practice incremental strategy per
  adapter).

### 3.3 Great Expectations

Added now, inside the dbt phase (per owner).

- Expectations on **bronze** (raw contract: columns present, ranges, nullability)
  and on **key gold marts** (row counts, referential integrity, freshness).
- Checkpoint runnable standalone **and** as a pre-push hook.
- Store validation results; fail the run on critical suites.

**Done when:** `dbt build` passes on ClickHouse; snapshots capture 311 status
changes; GE checkpoint green on bronze + gold; pre-push runs `dbt test` + GE.

**Interview topic:** dimensional modeling, SCD, incremental loads, data contracts,
data quality gates, lineage.

---

## 4. Phase 2 — Orchestration: Airflow (dockerized, Astronomer-ready)

Scope: turn the manual CLI run into a scheduled, tested DAG.

- **Astronomer Astro Runtime** (`astro dev start`) as the dockerized local
  environment → later deployable to Astronomer without a rewrite. Fallback:
  Airflow standalone service in `docker-compose.yaml`.
- DAG (TaskFlow API):
  `dlt ingest (bronze) → dbt run (silver/gold) → dbt test → GE checkpoint →
  publish/notify`
- Use: retries + exponential backoff, SLA, catchup/backfill, **Datasets/Assets**
  for event-driven scheduling, alerting on failure.
- Wire dbt into Airflow via **astronomer-cosmos** (per-model tasks + lineage) or
  `BashOperator` for the simple path.
- **Data tests live in the DAG** (owner requirement): `dbt test` and GE checkpoint
  are first-class tasks that gate the run.

**Done when:** DAG runs end-to-end on a schedule; a deliberately broken model
fails the GE/dbt task and alerts; a backfill reproduces a past interval.

**Interview topic:** DAG design, idempotency, retries/backfill, dependency graph,
SLA/alerting, orchestration of dbt + quality.

---

## 5. Phase 3 — Deploy: local (CH + Trino) vs cloud (GCS + BigQuery)

Scope: formalize the two-environment split. Local stack stays ClickHouse + Trino
over GCS; the deployed stack is GCS + BigQuery with **no Trino**.

### 5.1 Environment matrix

| Tier | Local (dev) | Cloud (prod) |
|---|---|---|
| Object store | **GCS** | **GCS** (bucket per env) |
| Bronze | dlt → GCS parquet | dlt → GCS parquet |
| Warehouse | **ClickHouse** (Docker) | **BigQuery** |
| Query engine | **Trino** (CH + Iceberg-on-GCS) | **BigQuery** (engine under the hood) |
| dbt target | `dbt-clickhouse` | `dbt-bigquery` |
| dlt destination | `clickhouse` | `bigquery` (+ `filesystem`→GCS staging) |
| Orchestration | Airflow (Docker / Astro) | Airflow (Docker / Astro) — Composer too costly |

Trino locally is the stand-in for BigQuery's engine in the cloud; Iceberg-on-GCS
locally becomes BigLake / Iceberg external tables in BigQuery, so the lakehouse
story carries over without a rewrite.

### 5.2 Changes to make

- **Drop MinIO** from `docker-compose.yaml`; point dlt + Trino at GCS.
  - Trino: `fs.native-gcs.enabled=true` + service account (or HMAC), Iceberg
    warehouse `gs://<bucket>/warehouse`; metastore stays local Postgres (or an
    Iceberg REST catalog).
  - dlt: `filesystem` destination with `bucket_url=gs://…` (add `dlt[gs]` /
    google-cloud deps).
- **dbt profiles**: `dev` → ClickHouse, `prod` → BigQuery. One model tree.
- **GCS buckets**: one per environment (`da-zoomcamp-dev`, `da-zoomcamp-prod`)
  with lifecycle rules (bronze → Coldline, expire raw after N days).
- Secrets: **Secret Manager** in cloud; `.env` / `.dlt/secrets.toml` locally.
- BigQuery table design: partitioning + clustering on the gold marts.

**Free-tier map (verify current limits before relying on them)**

| Service | Free allowance | Use |
|---|---|---|
| GCS | ~5 GB (US regions) | bronze + Iceberg warehouse, both envs |
| BigQuery | ~1 TiB queries + ~10 GiB storage / month | cloud gold warehouse |
| Secret Manager | small free tier | cloud credentials |
| **Cloud Composer** | **paid** (~$300+/mo) — do **not** use | keep Airflow local/Astro |

Note: there is no managed ClickHouse on a free tier — ClickHouse stays local
(Docker) by design. Dataflow/Pub/Sub are outside the free tier for sustained use —
prefer local Flink (Phase 4).

**Done when:** local `dbt build` runs on ClickHouse reading GCS; cloud `dbt build`
runs on BigQuery; the same models produce equivalent gold marts in both.

**Interview topic:** environment parity, managed vs self-hosted, IAM, GCS layout,
partitioning/clustering, cost, federated query (local) vs native engine (cloud).

---

## 6. Phase 4 — Streaming: Kafka + Flink (one phase)

Scope: event-time processing, exactly-once, state — together, not sequentially.

- **Kafka** (KRaft, single broker) in `docker-compose.yaml`.
- Producer: taxi-trip event simulator (or replay of parquet as events).
- **Flink** job — prefer **Flink SQL** for speed, PyFlink if Python parity matters:
  events → bronze/silver; watermarks, tumbling/sliding windows, late-data
  handling, checkpointing, exactly-once.
- Sinks: ClickHouse and/or Iceberg-on-GCS.
- Optional GCP analogue (Pub/Sub + Dataflow) noted, not required.

**Done when:** a stream flows Kafka → Flink → ClickHouse with correct windows;
killing the job and resuming does not duplicate or lose events.

**Interview topic:** streaming semantics, event-time vs processing-time, watermarks,
state/checkpointing, exactly-once.

---

## 7. Phase 5 — Distributed compute: PySpark (decoupled, standalone)

Decoupled from the agent work and kept as its own phase.

- PySpark batch over parquet (GCS): DataFrame API, multi-way joins, window
  functions, partition pruning, shuffle/skew handling, broadcast joins, AQE.
- Run locally (Spark container) + one managed target: **Databricks Community
  Edition** (free) or Dataproc Serverless / EMR.
- Contrast write-up: polars (single-node, already a dep) vs Spark (distributed) —
  when each wins.

**Done when:** a Spark job reproduces a gold mart and the README explains the
partitioning/skew decisions.

**Interview topic:** distributed compute, shuffle, partitioning, skew, broadcast,
OOM tuning, Spark vs single-node.

---

## 8. Phase 6 — Data agent (deferred): A2A + DSPy + OAuth2

Deferred until the data platform is solid. This is the phase that gates the
original "Phase 4" intent.

- **A2A server** (Agent2Agent protocol): agent card + skills exposed over HTTP.
- **DSPy** programs for NL→SQL / text-to-pipeline over ClickHouse/Trino; a
  metric-driven eval harness.
- **OAuth2** in front of the A2A endpoint (client credentials / auth-code) —
  agents authenticate before invoking skills.
- Tools: expose dbt models / ClickHouse queries as agent tools.

**Done when:** an authenticated A2A client invokes a DSPy-backed skill that
answers a data question and the eval harness scores it.

**Interview topic:** agent protocols, tool use, evals, auth for machine callers.

---

## 9. Phase 7 — IaC: Terraform (last of the build phases)

- Provision: GCS buckets (dev/prod), BigQuery datasets, service account + IAM
  bindings, Secret Manager entries. No ClickHouse/Trino in the cloud — they are
  local-only.
- **Remote state in GCS**; workspaces for dev/prod.
- `terraform plan` on PR, `apply` on main (CI).

**Done when:** `terraform apply` rebuilds the GCP footprint from zero.

**Interview topic:** infra-as-code, remote state, environments, CI-driven infra.

---

## 10. Phase 8 — Capstone

- End-to-end: source → GCS → BigQuery → dbt → BI.
- BI: **Looker Studio** (free on BigQuery) or Metabase/Superset locally.
- CI/CD deploys the pipeline; architecture diagram; README with design decisions
  and tradeoffs; short demo.
- Harvest each phase's diagram box and STAR story into `interview-prep/`.

**Done when:** a fresh clone + documented steps reproduces the full pipeline, and
the design is explainable end-to-end in a system-design round.

---

## 11. Timebox — a few intense days

Realistic cut for the stated window:

- **Day 1** — Phase 0 (prek) + Phase 1 start: dataset expansion (zone seed, 311,
  weather), dbt project skeleton, staging models.
- **Day 2** — Phase 1 finish: intermediate + marts, snapshots, dbt tests, GE
  checkpoint wired into pre-push; then Phase 2 (Airflow DAG).
- **Day 3** — Phase 3 (GCS + BigQuery + dbt-bigquery + Trino federation);
  hard cut-line.
- **Stretch** — Phase 4 (Kafka + Flink) and Phase 5 (PySpark).
- **Deferred** — Phase 6 (agent), Phase 7 (Terraform), Phase 8 (capstone).

If only **3 days** exist: Phases 0–3 only. That still covers the two most-asked
topics (orchestration + modeling) and the cloud migration story EPAM sells.

---

## 12. Interview-topic → phase map

| Interview topic | Phase |
|---|---|
| Testing, CI, local quality gates | 0 |
| Dimensional modeling, SCD, incremental, data contracts | 1 |
| Data quality / observability | 1 (+6 for lineage) |
| Orchestration: DAG, retries, backfill, SLA, alerting | 2 |
| Cloud: managed services, IAM, partitioning, cost, federation | 3 |
| Streaming: event-time, watermarks, exactly-once, state | 4 |
| Distributed compute: shuffle, skew, partitioning, tuning | 5 |
| Agent protocols, tool use, evals, machine auth | 6 |
| IaC, remote state, environments | 7 |
| System design (present the whole thing) | 8 |

---

## 13. Artifacts checklist (harvest per phase)

- [ ] Architecture diagram — one box per phase
- [ ] README "Design Decisions" section — becomes the system-design answer
- [ ] One STAR story per phase in `interview-prep/story-bank.md`
- [ ] `dbt docs` site (lineage) — Phase 1
- [ ] Airflow graph screenshot — Phase 2
- [ ] GCP cost/architecture notes — Phase 3
- [ ] Flink job + Kafka topic screenshot — Phase 4
- [ ] Spark tuning write-up (skew/partition) — Phase 5
- [ ] Terraform plan output — Phase 7
- [ ] Capstone demo + BI dashboard — Phase 8
