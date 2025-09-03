# ADR 0005: Concentration Contract and Threshold Semantics

Status: Accepted
Date: 2025-09-03

## Context
The initial implementation hardcoded `top_10/top_20/top_50` fields. We needed a deterministic, extensible contract that supports arbitrary thresholds while keeping outputs auditable and consistent across single- and multi-period analyses.

## Decision
- Thresholds are dynamic percentage cutoffs; validator accepts 1–100 inclusive, sorts ascending, and deduplicates.
- Semantics: for each threshold X, include entities whose cumulative share ≤ X%; if none qualify, include at least 1 entity.
- Deterministic ranking and tie-breaking: ORDER BY value DESC, then `group_by` ASC.
- Periods: analyze each period (if time exists) and include an overall aggregate labeled `TOTAL`. Historical `ALL` values are tolerated on read but not produced.
- API response: per-period `concentration` is a dictionary keyed by `top_{X}` (e.g., `top_5`, `top_15`, `top_100`); per-period `head` is included (capped for payload size), and `totals` mirrors the same structure.

## Rationale
- Avoid hardcoded fields to enable flexible thresholds and future evolution without breaking clients.
- Enforce deterministic ordering for reproducibility and testability.
- Provide a single, consistent aggregate label across single- and multi-period workflows.

## Consequences
- Clients iterate dynamic keys (e.g., `top_5`, `top_15`, `top_100`) rather than fixed fields.
- Thresholds appear in sorted order in API responses and exports.
- `top_100` represents all entities with `pct_of_total` ≈ 100.0.

## References
- Time detection precedence and `period_key` composition per ADR 0004.
- Implemented in: `core/deterministic/concentration.py`, `api/v1/models.py`, `api/v1/routes.py`.

