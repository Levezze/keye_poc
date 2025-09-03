# Technical Debt Register - Keye POC

This document tracks technical debt items organized by priority and branch. Each item includes rationale, effort estimates, and actionable checklists.

## Current Active Debt (By Priority)

### High Priority

*No high priority technical debt items currently identified*

### Low Priority

#### API Versioning Strategy
**Background:** Recent enhancements to threshold handling (1-100% range, automatic sorting) and performance optimizations suggest future API evolution needs.

**Current State:**
- All endpoints under `/api/v1/` with backward compatibility maintained
- Dynamic threshold validation and sorting implemented
- Performance warnings for large datasets (>10k entities)
- Optional timing metrics via configuration

**Recommendation:**
- Document API versioning strategy (semver vs date-based)
- Consider v1.1 announcement for enhanced threshold features
- Plan deprecation timeline for any breaking changes

### Minor Issues

#### Performance Scalability for Large Datasets
**Location:** `core/deterministic/concentration.py:214-221`

**Current Implementation:**
- Large dataset warnings trigger at >10k entities (configurable threshold)
- Warning logged but processing continues with full in-memory operations

**Issue:**
- Performance consideration: warnings are helpful, but consider implementing actual pagination or chunking for datasets >10k entities

**Proposed Solution:**
- Implement streaming/chunked processing for very large datasets
- Consider processing in batches for memory efficiency
- Add optional pagination for head samples in API responses

#### Memory Efficiency in Head Sample Processing
**Location:** `core/deterministic/concentration.py:258-263`

**Current Implementation:**
```python
head_df = grouped_sorted.head(min(20, len(grouped_sorted))).copy()
# Vectorized conversion of numeric columns to float for JSON serialization
for col in head_df.columns:
    if pd.api.types.is_numeric_dtype(head_df[col]):
        head_df[col] = head_df[col].astype(float)
head_sample = head_df.to_dict("records")
```

**Issue:**
- The head sample limits to 20 items but converts entire DataFrame copy
- Memory inefficient for large datasets

**Proposed Solution:**
```python
# Consider direct conversion of limited rows without full DataFrame copy
# Process only the subset needed for head sample
```

#### Export Error User Feedback
**Location:** `api/v1/routes.py:355-365`

**Current Implementation:**
- Export failures are logged to computation log
- API continues without export files
- Users don't receive clear indication of export issues

**Issue:**
- Export failures are logged but don't surface clearly to the user
- Response shows `export_links: null` without explanation

**Proposed Solution:**
- Add warning in API response when exports fail
- Include export failure details in `warnings` array
- Consider partial success responses (e.g., CSV succeeded but Excel failed)

### Medium Priority

#### CSV Metadata Export Format
**Current Implementation:**
- CSV exports append metadata as trailing line: `GroupBy,{value}`
- Breaks standard CSV format and may confuse parsers

**Location:** `api/v1/routes.py` (concentration CSV download)

**Issues:**
- Non-standard CSV format (metadata mixed with data)
- Potential parsing issues in spreadsheet applications
- Inconsistent with proper CSV structure

**Proposed Solutions:**
1. **Option A: Metadata Columns** (Recommended for v1.1)
   - Add `group_by`, `value_column`, `analysis_date` columns to each row
   - Standard CSV format, self-documenting
   - Backwards compatible if we also keep the trailing line for one release
2. **Option B: Separate Metadata File** (Recommended for v2.0)
   - Generate `concentration_metadata.json` alongside `concentration.csv`
   - Clean separation of data and metadata
3. **Option C: CSV Header Comments**
   - Prepend `# GroupBy: entity`-style comments on first lines
   - Supported by some CSV consumers but not all

**Migration Path:**
- Phase 1: Add proper columns while maintaining current append for BC
- Phase 2: Remove append behavior and document breaking change
- Phase 3: Consider metadata file approach for complex scenarios

**Effort Estimate:** 2-3 hours for Option A, 4-5 hours for Option B
**Risk:** Low - affects CSV export format only

#### Path Canonicalization
**Current Implementation:**
- Direct path joins without canonicalization helpers
- ID regex validation provides basic protection

**Issues:**
- No centralized path safety validation
- Potential security risk if regex validation insufficient

**Solution:**
- Add `_safe_dataset_path` helper that enforces paths within `datasets_path`
- Replace direct joins in routes with helper

**Effort Estimate:** 2-3 hours
**Risk:** Medium - security related

### Low Priority

#### Export Payload Size Management
**Issue:** API responses include full head samples which could grow large
**Solution:** Implement pagination or configurable limits
**Current Mitigation:** 10-item limit is reasonable for POC

#### Performance Optimization for Large Datasets
**Issue:** 10k entity limit may not be sufficient for enterprise use
**Solution:** Implement streaming or chunked processing
**Current Mitigation:** Performance adequate for expected POC use

#### Error Message Localization
**Issue:** All error messages are in English
**Solution:** Implement i18n framework
**Current Mitigation:** English sufficient for POC

## Debt by Feature Branch

### feature/registry-and-storage

- **Timestamps use naive ISO strings**
  - Reason: Avoid test churn while migrating from `utcnow()`
  - [ ] Migrate to `datetime.now(timezone.utc)` and emit `Z`
  - [ ] Update tests to parse timestamps instead of string equals

- **No listing/indexing endpoint for datasets**
  - Reason: POC scope keeps FS as source of truth
  - [ ] Optional: add lightweight index or listing API

### feature/normalization-and-schema

- **Precompiled regexes scattered across modules**
  - Reason: Readability and reuse optimization
  - [ ] Move shared patterns to `core/deterministic/patterns.py`
  - [ ] Import in normalization/time modules

- **Ambiguous date detection warnings could be richer**
  - Reason: Current behavior is deterministic; deeper heuristics optional
  - [ ] Add explicit "ambiguous day/month" counter when mixed formats detected

### feature/time-and-periods

- **No fiscal calendar support**
  - Reason: Out of scope for POC
  - [ ] Document override hooks for fiscal mapping
  - [ ] Add fiscal year start month configuration

- **`selected_time_columns` and `derivations` not exposed in API schema**
  - Reason: Internal metadata kept minimal for POC
  - [ ] Add selected_time_columns to schema response if debugging needs arise
  - [ ] Document derivation logic for manual override scenarios

- **No manual time column override mechanism**
  - Reason: POC focuses on automatic detection
  - [ ] Add optional time_column_hints parameter to normalization API
  - [ ] Allow explicit period_grain selection to override detection

- **Time warnings bundled with general warnings**
  - Reason: Simplified for POC
  - [ ] Consider separate time_warnings field for better UX categorization

### feature/concentration-analysis

- **Export formulas placeholder**
  - Reason: Formulas nice for audit but not required for POC
  - [ ] Implement basic Excel formulas in Summary sheet

- **No performance benchmarks**
  - Reason: POC data sizes are small
  - [ ] Add simple benchmark fixture for N=100k rows

### feature/api-v1

- **Rate limiting and timeouts**
  - Reason: Non-essential for POC
  - [ ] Add simple rate limit (proxy or middleware) and request timeout

- **Error model standardization**
  - Reason: Kept minimal for development speed
  - [ ] Add shared error responses with codes

### feature/llm-adapters (optional)

- **Insights are basic, schema notes minimal**
  - Reason: Advisory only; core math is deterministic
  - [ ] Persist richer context; add redact/scrub as needed

### chore/docker-and-docs

- **docs/api.md is a stub**
  - Reason: Time-boxed; Postman/pytest cover examples
  - [ ] Fill `docs/api.md` with curl examples and response shapes

- **Security hardening only partial**
  - Reason: POC scope; added API key and basic checks
  - [ ] Add dataset ID path canonicalization
  - [ ] Consider rate limiting note in docs

## Resolved Technical Debt

### ✅ Custom Thresholds Bug (Resolved v1.0)
- **Issue:** Custom thresholds silently dropped due to hard-coded API models
- **Solution:** Dynamic threshold support with `Dict[str, ConcentrationMetrics]`
- **Resolved:** Complete custom threshold pipeline implementation

### ✅ Pydantic Mutable Defaults (Resolved v1.0)
- **Issue:** Mutable defaults causing state leakage between requests
- **Solution:** `Field(default_factory=...)` pattern throughout codebase
- **Resolved:** All mutable defaults fixed

### ✅ Export Formatting (Resolved v1.0)
- **Issue:** Basic export formats without head samples
- **Solution:** Structured CSV sections, Excel Top_Entities sheet, head sample integration
- **Resolved:** Enhanced export formatting with proper data surfacing

### ✅ Input Validation (Resolved v1.0)
- **Issue:** Insufficient API request validation
- **Solution:** Comprehensive field validation with proper error messages
- **Resolved:** Robust validation for thresholds, field requirements

### ✅ Period Label Inconsistency (Resolved v1.0)
- **Issue:** Mix of "ALL" vs "TOTAL" across single/multi-period analysis
- **Solution:** Standardized on "TOTAL" with backwards compatibility
- **Resolved:** Consistent period labeling verified

### ✅ Error Message Standardization (Resolved v1.0)
- **Issue:** Inconsistent error messages for edge cases
- **Solution:** Standardized error messages with proper handling
- **Resolved:** "Total value is non-positive; cannot compute concentration" pattern

## Global Follow-ups

Priority order for addressing remaining debt:

1. **Path Canonicalization** (Medium) - Security related
2. **CSV Metadata Export** (Medium) - Standards compliance  
3. **Timestamp Migration** (Low) - Code quality
4. **Performance Benchmarking** (Low) - Future-proofing
5. **Documentation Completion** (Low) - User experience

---

*Last Updated: 2025-01-03 (Submission Ready)*
*Next Review: When starting new feature branches*