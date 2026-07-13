# FX Data Pipeline

End-to-end data engineering pipeline demonstrating a production-style medallion architecture: live FX rates are ingested from a public API, transformed through a bronze → staging → intermediate → marts pipeline in dbt, orchestrated daily by Airflow, and validated on every push via GitHub Actions CI. Fully containerized with Docker.

**Author:** Peter Obert — [github.com/obino131](https://github.com/obino131)

## Status

✅ Phases 0–4 complete. Live, orchestrated, tested, CI-validated pipeline.

Bonus Phase 5 (PySpark + Databricks) is a planned extension, not required for the core pipeline to function.

## Architecture

~~~
┌─────────────────────┐
│   Frankfurter API    │  (ECB reference rates, daily, no API key)
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  Python ingestion    │  ingestion/fetch_fx.py
│  (config, logging,   │  → append-only bronze layer
│   error handling)    │
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│   Postgres (bronze)  │  bronze_fx_rates
└──────────┬───────────┘
           │
           ▼
┌─────────────────────────────────────────────┐
│                  dbt Core                     │
│                                                │
│  staging (view)                               │
│    stg_fx_rates — DISTINCT ON deduplication   │
│           │                                    │
│           ▼                                    │
│  intermediate (view)                          │
│    int_fx_rate_changes — LAG() window fn      │
│           │                                    │
│           ▼                                    │
│  marts (table)                                │
│    fct_latest_fx_rates — ROW_NUMBER()         │
│                                                │
│  19 dbt tests (not_null, accepted_values,     │
│  singular uniqueness tests)                    │
└──────────┬─────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────┐
│              Apache Airflow 3.1.2             │
│  DAG: fx_pipeline (@daily)                    │
│  ingest_fx_rates → dbt_run → dbt_test         │
│  LocalExecutor, isolated dbt venv,            │
│  separate metadata Postgres                    │
└─────────────────────────────────────────────┘

           ┌──────────────────────────┐
           │   GitHub Actions CI       │
           │   On every push:          │
           │   pytest + dbt build      │
           │   against Postgres        │
           │   service container       │
           └──────────────────────────┘
~~~

## Tech stack & key decisions

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python (`requests`, `psycopg2`) | Fail-fast config, structured logging, unit-tested parsing logic |
| Storage | PostgreSQL 16 | Pinned version for reproducibility; separate instances for pipeline data vs. Airflow metadata |
| Transformation | dbt Core 1.11 | Medallion architecture (bronze → staging → intermediate → marts); deduplication and business logic live here, not in ingestion |
| Orchestration | Apache Airflow 3.1.2 | LocalExecutor (single-machine, no need for Celery/Redis); custom Docker image with an isolated virtualenv for dbt to avoid dependency conflicts with Airflow's own Python environment |
| CI/CD | GitHub Actions | Runs unit tests and a full `dbt build` against a fresh Postgres service container on every push |
| Containerization | Docker Compose | Every service (data Postgres, Airflow metadata Postgres, Airflow services) is isolated and reproducible |

For the full reasoning behind each decision — including problems encountered and how they were diagnosed and fixed — see [`docs/ENGINEERING_LOG.md`](docs/ENGINEERING_LOG.md).

## Local setup (from scratch)

**Prerequisites:** Docker Desktop with WSL2 integration (if on Windows), Python 3.12, Git with SSH access configured.

1. Clone the repo:

   ~~~bash
   git clone git@github.com:obino131/fx-data-pipeline.git
   cd fx-data-pipeline
   ~~~

2. Copy environment file and adjust if needed:

   ~~~bash
   cp .env.example .env
   ~~~

3. Set up the Python virtual environment (for local dbt/ingestion development, separate from what runs inside Airflow containers):

   ~~~bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ~~~

4. Create your local dbt profile (not committed — contains connection info):

   ~~~bash
   mkdir -p ~/.dbt
   cat > ~/.dbt/profiles.yml << 'PROFILE'
   fx_transform:
     outputs:
       dev:
         type: postgres
         host: "{{ env_var('DBT_POSTGRES_HOST', 'localhost') }}"
         port: 5432
         user: fx_user
         pass: changeme
         dbname: fx_pipeline
         schema: public
         threads: 4
     target: dev
   PROFILE
   ~~~

5. Start everything (data Postgres + Airflow metadata Postgres + Airflow services):

   ~~~bash
   docker compose up -d
   docker compose ps   # wait for all services to report healthy
   ~~~

6. Run ingestion + dbt manually (optional — you can also trigger the Airflow DAG instead):

   ~~~bash
   python -m ingestion.fetch_fx
   cd dbt && dbt build
   ~~~

7. Access Airflow UI at [http://localhost:8080](http://localhost:8080).
   Username: `airflow`. Password is auto-generated on first startup — retrieve it with:

   ~~~bash
   docker compose logs airflow-apiserver | grep -i "password for user"
   ~~~

   Trigger the `fx_pipeline` DAG manually from the UI to run the full `ingest → dbt run → dbt test` chain.

## Running tests

~~~bash
# Python unit tests
pytest tests/ -v

# dbt tests (staging/intermediate/marts data quality)
cd dbt && dbt build
~~~

Both also run automatically on every push via GitHub Actions (see `.github/workflows/ci.yml`).

## Roadmap

- [x] Phase 0 — Repo, Docker, Postgres
- [x] Phase 1 — Python ingestion (bronze layer)
- [x] Phase 2 — dbt transformations (staging/intermediate/marts, 19 tests)
- [x] Phase 3 — Airflow orchestration (LocalExecutor, isolated dbt venv)
- [x] Phase 4 — CI/CD (GitHub Actions) + documentation
- [ ] Phase 5 (bonus) — PySpark + Databricks Free Edition

## Engineering log

Detailed reasoning, problems encountered, and how they were diagnosed and fixed, phase by phase: [`docs/ENGINEERING_LOG.md`](docs/ENGINEERING_LOG.md).
