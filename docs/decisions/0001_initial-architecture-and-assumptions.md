# ADR 0001: Initial Architecture and Assumptions (POC)

Status: Accepted
Date: 2025-05-30

## Context
Keye POC requires: upload Excel/CSV, auto-detect schema, normalize (generic + domain rules), handle composite time periods, run concentration analysis (10/20/50%), and generate AI insights. Auditability, accuracy, and Dockerized demo are emphasized.

## Decision
- API-only FastAPI service, versioned at `/api/v1`.
- Accept both Excel and CSV uploads; outputs are JSON (API) with downloadable CSV/Excel mirrors.
- Canonical storage is Parquet under per-dataset folders in `storage/datasets/{dataset_id}/`.
- Persist `schema.json` (deterministic schema + optional LLM notes) and `lineage.json` (append-only provenance) per dataset.
- Deterministic concentration analysis in code; thresholds configurable; per-period when a time dimension exists; graceful single-period otherwise.
- Composite time handling: prefer `date` > (`year`+`month`) > (`year`+`quarter`) > `year`. Canonical `period_grain` and `period_key` (`YYYY-M02`, `YYYY-Q1`, `YYYY`, or `ALL`).
- LLM usage limited to optional schema descriptions and narrative insights; never computes or verifies metrics.
- No DB for the POC; optionally add a lightweight SQLite index for listing/search if time permits. Filesystem + JSON remain authoritative.

## Rationale
- Determinism and auditability demand code-driven math and explicit lineage; Parquet is portable and typed.
- JSON + filesystem artifacts are fast to implement and easy to review in a POC; SQLite index is a non-invasive enhancement.
- Single-shot LLMs reduce latency/cost and avoid multi-step fragility while still showcasing AI value.
- Composite time primitives are common; a clear `period_grain`/`period_key` contract keeps sorting/aggregation correct and explainable.

## Consequences
- Easy to run and audit (Parquet + JSON). Clear future path to S3/Postgres without changing business logic.
- LLM annotations are advisory; analysis inputs remain explicit and deterministic.
- Graceful no-time behavior returns a single-period analysis with warnings instead of failing.

## Alternatives Considered
- Pickled DataFrames (rejected: opaque, insecure, version fragile).
- Full agent/orchestration framework (rejected for POC: overkill, higher risk).
- Database-first design (rejected for POC speed; may adopt later for multi-user/history queries).

## Open Questions (closed by clarification)
- Input/output: Excel+CSV accepted; JSON API + CSV/Excel mirrors are sufficient.
- Normalization/anomalies: include domain-specific rules in addition to generic cleanup.
- Time behavior: prefer Year+Month; handle composites; fail gracefully when absent.
- Persistence: filesystem artifacts are minimum; document extension to cloud/object store and DB index.
