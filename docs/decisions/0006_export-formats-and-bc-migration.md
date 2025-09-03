# ADR 0006: Export Formats and Backward-Compatibility Migration

Status: Accepted
Date: 2025-09-03

## Context
Early CSV exports were ad hoc and appended a trailing `GroupBy,` line for quick metadata. We standardized the formats for tool-friendliness while providing a migration path to avoid breaking existing consumers.

## Decision
- CSV: single-table schema with columns `period, threshold, count, value, pct_of_total` (percentage rounded to 1 decimal). Threshold rows are emitted in ascending numeric order.
- Excel: three sheets — `Summary` (dynamic threshold columns), `Top_Entities` (ranked head per period), `Parameters` (config/inputs). Both nested `concentration` dicts and legacy `top_*` keys are supported when constructing sheets.
- Backward compatibility: The CSV download route continues to append a trailing `GroupBy,` line for one release window. The recommended path forward is to move metadata into proper CSV columns or a sidecar JSON file. The trailing line will be removed after the BC window.

## Rationale
- Provide predictable, machine-/human-friendly exports with stable columns and ordering.
- Allow a deprecation period so downstream consumers can migrate safely.

## Consequences
- Clients can rely on standardized CSV and Excel outputs immediately.
- A documented deprecation path replaces the trailing metadata line with proper columns or a sidecar.

## References
- Concentration contract per ADR 0005.
- Implemented in: `services/exporters.py`, `api/v1/routes.py` (download behavior).

