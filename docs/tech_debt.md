# Technical Debt - Keye POC

This document tracks technical debt items for future improvement.

## CSV Metadata Export (Priority: Medium)

**Current Implementation:**
- CSV exports append metadata as a trailing line: `GroupBy,{value}`
- This approach breaks standard CSV format and may confuse CSV parsers

**Location:** 
- `api/v1/routes.py:474-494` (concentration CSV download)

**Issues:**
- Non-standard CSV format (metadata mixed with data)
- Potential parsing issues in spreadsheet applications
- Inconsistent with proper CSV structure

**Proposed Solutions:**
1. **Option A: Metadata Columns** (Recommended for v1.1)
   - Add `group_by`, `value_column`, `analysis_date` columns to each row
   - Standard CSV format, self-documenting
   - Easy migration path

2. **Option B: Separate Metadata File** (Recommended for v2.0)
   - Generate `concentration_metadata.json` alongside `concentration.csv`
   - Clean separation of data and metadata
   - More flexible for complex metadata

3. **Option C: CSV Header Comments**
   - Use `# GroupBy: entity` style comments at top of file
   - Some CSV parsers support this pattern

**Migration Path:**
- Phase 1: Add proper columns while maintaining current append for BC
- Phase 2: Remove append behavior and document breaking change
- Phase 3: Consider metadata file approach for complex scenarios

**Effort Estimate:** 2-3 hours for Option A, 4-5 hours for Option B

**Risk Assessment:** Low risk - mainly affects CSV export format

---

## Future Tech Debt Candidates

### 1. Export Payload Size Management
**Issue:** API responses include full head samples which could grow large
**Solution:** Implement pagination or configurable limits
**Priority:** Low (current limit of 10 items is reasonable)

### 2. Error Message Localization  
**Issue:** All error messages are in English
**Solution:** Implement i18n framework for error messages
**Priority:** Low (English sufficient for POC)

### 3. Performance Optimization for Large Datasets
**Issue:** 10k entity limit may not be sufficient for enterprise use
**Solution:** Implement streaming or chunked processing
**Priority:** Low (current performance adequate for expected use)

---

## Resolved Tech Debt

### ✅ Period Label Inconsistency (Resolved v1.0)
- **Issue:** Mix of "ALL" vs "TOTAL" across single/multi-period
- **Solution:** Standardized on "TOTAL" with API backwards compatibility
- **Resolved:** Phase 1.1 implementation

### ✅ Threshold Semantic Documentation (Resolved v1.0)
- **Issue:** Unclear threshold behavior (≤ X% with at least 1 entity rule)
- **Solution:** Explicit documentation and examples in code and tests
- **Resolved:** Phase 1.2 implementation

### ✅ Error Message Standardization (Resolved v1.0)
- **Issue:** Inconsistent error messages for edge cases
- **Solution:** Standardized to "Total value is non-positive; cannot compute concentration"
- **Resolved:** Phase 1.3 implementation