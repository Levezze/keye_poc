# ADR 0003: Caching Strategy for Scaling

Status: Accepted
Date: 2025-09-02

## Context
The POC persists normalized datasets and analysis artifacts (JSON/CSV/Excel) under `storage/datasets/{dataset_id}/`. This acts as a natural cache for re-use. As the system scales (more datasets, repeated analyses), a dedicated cache improves responsiveness and reduces recomputation and I/O.

## Decision
- POC: do not add a cache. Rely on persisted artifacts and short-circuit recomputation when artifacts already exist for the same parameters.
- Production path: introduce Redis as an application cache for:
  - Analysis result lookups keyed by `(dataset_id, group_by, value, period_grain, thresholds_hash)`
  - Small metadata (e.g., `schema.json` summary, `period_grain`, `time_candidates`)
  - Signed/absolute paths to latest artifacts (JSON/CSV/Excel)
- Invalidation strategy:
  - On new upload/normalization: invalidate all keys for `dataset_id`
  - On analysis write: upsert cache entries for that parameter set; set TTL
- TTL policy: short (e.g., 1–6 hours) by default; extend for cold data if needed.

## Rationale
- Redis provides low-latency lookups, robust data structures, and operational maturity. It complements file-based persistence without changing core contracts (Parquet + JSON).
- Keeps compute idempotent and transparent: cache is a fast path; persisted artifacts remain the source of truth.

## Consequences
- App layer complexity increases slightly (cache client + keys + invalidation), but downstream callers see faster response times when results are reused.
- Cache outages degrade gracefully (fallback to persisted artifacts and recomputation when necessary).

## Alternatives Considered
- No cache (rely solely on artifacts): simplest; may be sufficient for low throughput.
- CDN for static artifact URLs: useful once artifacts are public; does not help with metadata/lookup patterns.
- Application-level LRU in-process: fast but not multi-instance safe; resets on deploy.

## Migration Plan
1) Define cache keys and value schemas (JSON) for:
   - `analysis:{dataset_id}:{hash(params)}` → summary payload + artifact paths
   - `schema_summary:{dataset_id}` → selected fields from `schema.json`
2) Add a small `services/cache.py` with a Redis client (env-configured URL), plus `get/set/delete` helpers.
3) Wrap analysis read path to consult cache first, then populate on miss.
4) Wire invalidation in registry steps:
   - `create_dataset`/`normalize` → delete `analysis:*` and `schema_summary` keys for dataset
   - `save_schema` → update `schema_summary:{dataset_id}`
5) Monitor hit rate and adjust TTL.

## Out of Scope (POC)
- Redis deployment and configuration
- CDN and edge caching
- Cross-region cache replication
