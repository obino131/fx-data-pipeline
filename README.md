# FX Data Pipeline

End-to-end data engineering pipeline: live FX rates → Python ingestion (bronze layer, Postgres) → dbt transformations → Airflow orchestration → CI/CD via GitHub Actions. Fully containerized.

## Status
🚧 Work in progress — Phase 3 complete (Airflow: ingest → dbt run → dbt test, orchestrated end-to-end).

## Architecture
_TODO — diagram and explanation coming in later phases._

## Local setup (current: Postgres only)
1. Copy `.env.example` to `.env`
2. `docker compose up -d`
3. Connect: `docker compose exec postgres psql -U fx_user -d fx_pipeline`

## Roadmap
- [x] Phase 0 — Repo, Docker, Postgres
- [x] Phase 1 — Python ingestion (bronze)
- [x] Phase 2 — dbt transformations (staging/intermediate/marts)
- [x] Phase 3 — Airflow orchestration
- [ ] Phase 4 — CI/CD + final docs
- [ ] Phase 5 (bonus) — PySpark + Databricks
