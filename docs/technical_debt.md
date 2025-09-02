# Technical Debt Register

Purpose: Track intentional deferrals and polish items by branch. Each item lists a short rationale and a small checklist to clear at the end.

## feature/registry-and-storage

- Debt: Timestamps use naive ISO strings in some places
  - Reason: Avoid test churn while migrating away from `utcnow()`
  - Checklist:
    - [ ] Migrate to `datetime.now(timezone.utc)` and emit `Z`
    - [ ] Update tests to parse timestamps instead of string equals

- Debt: No path canonicalization helpers in API (ID format validation added)
  - Reason: ID regex already constrains inputs; canonicalization is belt-and-suspenders
  - Checklist:
    - [ ] Add `_safe_dataset_path` that enforces paths within `datasets_path`
    - [ ] Replace direct joins in routes with helper

- Debt: No listing/indexing endpoint for datasets
  - Reason: POC scope keeps FS as source of truth; list not required
  - Checklist:
    - [ ] Optional: add lightweight index or listing API

## feature/normalization-and-schema

- Debt: Precompiled regexes exist; consider extracting to constants module
  - Reason: Readability and reuse across modules
  - Checklist:
    - [ ] Move shared patterns to `core/deterministic/patterns.py`
    - [ ] Import in normalization/time modules

- Debt: Ambiguous date detection warnings could be richer
  - Reason: Current behavior is deterministic; deeper heuristics are optional
  - Checklist:
    - [ ] Add explicit "ambiguous day/month" counter when mixed formats detected

## feature/time-and-periods

- Debt: No fiscal calendar support
  - Reason: Out of scope for POC
  - Checklist:
    - [ ] Document override hooks for fiscal mapping

- Debt: `period_key` head sample not surfaced in API responses
  - Reason: Keep payloads small for POC
  - Checklist:
    - [ ] Consider adding compact head sample for debugging

## feature/concentration-analysis

- Debt: Export formulas placeholder
  - Reason: Formulas are nice for audit but not required
  - Checklist:
    - [ ] Implement basic Excel formulas in Summary sheet

- Debt: No performance benchmarks
  - Reason: POC data sizes are small
  - Checklist:
    - [ ] Add simple benchmark fixture for N=100k rows

## feature/api-v1

- Debt: Dataset ID path canonicalization (validation added)
  - Reason: Regex validation covers primary risk
  - Checklist:
    - [ ] Add `_safe_dataset_path` and switch all path joins

- Debt: Rate limiting and timeouts
  - Reason: Non-essential for POC
  - Checklist:
    - [ ] Add simple rate limit (proxy or middleware) and request timeout

- Debt: Error model standardization
  - Reason: Kept minimal for speed
  - Checklist:
    - [ ] Add shared error responses with codes

## feature/llm-adapters (optional)

- Debt: Insights are basic, schema notes minimal
  - Reason: Advisory only; core math is deterministic
  - Checklist:
    - [ ] Persist richer context; add redact/scrub as needed

## chore/docker-and-docs

- Debt: docs/api.md is a stub
  - Reason: Time-boxed; Postman/pytest cover examples
  - Checklist:
    - [ ] Fill `docs/api.md` with curl examples and response shapes

- Debt: Security hardening only partial
  - Reason: POC; added API key and basic checks
  - Checklist:
    - [ ] Add dataset ID path canonicalization
    - [ ] Consider rate limiting note in docs

---

Global follow-ups

- [ ] Replace utcnow() usage with timezone-aware now + `Z`
- [ ] Add `_safe_dataset_path` helper and adopt across routes
- [ ] Light rate limiting and request timeouts (documented defaults)
- [ ] Performance benchmark note for large files (streaming/async path)

