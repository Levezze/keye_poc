# Financial Document Analysis Strategies

## Executive Summary (for reviewers)
- Inputs: Excel/CSV in the wild; we normalize deterministically into an analysis‑ready Parquet.
- Outputs: A clear concentration analysis (top 10/20/50%) by period, downloadable as JSON/CSV/Excel.
- Guarantees: Reproducible math; auditable steps via `schema.json` and `lineage.json`.
- AI usage: Optional and advisory (schema descriptions, insights); never computes numbers.

## 1. Purpose

Financial data analysis demands **deterministic, auditable processing** to ensure regulatory compliance, reproducible results, and trustworthy insights. Every transformation must be traceable, every calculation verifiable, and every anomaly documented.

This POC demonstrates production-ready patterns for ingesting messy Excel/CSV files and transforming them into normalized, analyzable datasets with complete audit trails. The architecture prioritizes **correctness over speed** and **transparency over complexity**, establishing a foundation that scales from proof-of-concept to production deployment.

## 2. Schemas in the Wild

Financial data arrives in diverse formats reflecting organizational workflows:

**Wide vs Long Format:**
- **Wide**: Columns represent time periods (`Q1_2024`, `Q2_2024`); common in P&L statements
- **Long**: Single value column with period dimension (`period`, `amount`); standard for transactional data

**Typical Column Patterns:**
- Identity: `customer_name`, `product_id`, `region`, `business_unit`
- Time: `date`, `year`, `month`, `quarter`, `fiscal_period`
- Metrics: `revenue`, `cost`, `margin`, `units_sold`
- Metadata: `currency`, `status`, `notes`

**Naming Variance:**
Headers vary wildly (`Revenue (USD)`, `Rev_Q1`, `REVENUE_2024`), requiring robust normalization. Multi-word headers, special characters, and inconsistent casing are the norm, not the exception.

## 3. Normalization Rules (Deterministic)

### Currency Parsing
- Strip symbols (e.g., `$`, `€`, `£`), thousands separators, and whitespace
- Parentheses indicate negatives: `(100)` → `-100`
- Suffix scaling: `k` = ×1,000; `m` = ×1,000,000; `b` = ×1,000,000,000
- Multi-currency detection: flag mixed currencies in schema warnings (no FX conversion in this POC)

### Percent Normalization
- Detect `%` suffix or values > 1 in percent-named columns
- Normalize to decimal `[0,1]`: `85%` → `0.85`
- Schema marks `representation: "percent"` for export formatting

### Negative Value Policy
**Allowlist approach** for negative-allowed columns:
- Revenue metrics: flag negatives (likely returns/adjustments)
- Cost/expense columns: allow negatives (credits/reversals)
- Margin columns: allow negatives (unprofitable segments)

### Header Cleanup
1. Trim whitespace, convert to lowercase
2. Replace special chars with underscores
3. Deduplicate with numeric suffixes (`revenue`, `revenue_2`)
4. Apply snake_case convention

## 4. Time Dimension Handling

### Detection & Precedence
- Precedence order (most specific first): date → year+month → year+quarter → year → none
- Name detection uses word boundaries (e.g., “year”, “month”, “quarter”), then value validation
- Composite derivation: From date columns, derive year/month/quarter

### Period Key Contracts
- `YYYY-M02`: Year-month (zero-padded month)
- `YYYY-Q1`: Year-quarter
- `YYYY`: Year only
- `ALL`: No time dimension detected

Additionally returned by the schema endpoint:
- `period_grain`: chosen grain by precedence (`year_month|year_quarter|year|none`)
- `period_grain_candidates`: all grains supported by available time columns (e.g., `["year_month","year_quarter","year"]`)
- `time_candidates`: columns that look temporal (e.g., `["date","year","month"]`)

### Sorting Strategy
Sort by tuples `(year, month/quarter)` instead of strings to ensure `2024-M02` < `2024-M10`.

### Graceful No-Time Behavior
When no time dimension is detected:
- Return single-period analysis with `period_key='ALL'`
- Add warning to schema response
- Continue processing without failure

Typical warnings surfaced:
- `"No temporal dimension detected; returning single-period analysis"`
- `"Multiple time candidates found; defaulted to date"`
- `"Inconsistent quarter vs month; using month and flagging mismatch"`

## 5. Concentration Analysis Semantics

### Core Definition
**Concentration**: Number of entities required to reach cumulative thresholds of total value.

Deterministic calculation conceptually:
- Sort entities by aggregated value (desc), compute cumulative sum and share of total
- For each threshold X in {10, 20, 50}, count entities while cumulative share ≤ X%

### Design Decisions
- **Independent buckets**: Each threshold calculated separately (not nested)
- **Inclusive boundaries**: Entity crossing threshold is included
- **Tie-breaking**: Sort by value desc, then key asc for determinism
- **Zero/negative handling**: Include in calculations with warnings

### Transparency
We include a small “head” sample in responses for quick manual verification.

Example response fragment:
```json
{
  "dataset_id": "ds_123",
  "period_grain": "year_month",
  "warnings": [],
  "thresholds": [10,20,50],
  "by_period": [
    {"period": "2024-M01", "total": 123456.78,
     "top_10": {"count": 11, "value": 12345.68, "pct_of_total": 10.0},
     "top_20": {"count": 23, "value": 24691.36, "pct_of_total": 20.0},
     "top_50": {"count": 57, "value": 61728.39, "pct_of_total": 50.0},
     "head": [{"entity": "ACME", "value": 2345.67}, {"entity": "BETA", "value": 2001.00}]}
  ]
}
```

## 6. Anomalies and Quality Signals

### Detection Categories
- **Structural**: Null rates > 50%, duplicate rows, empty columns
- **Type coercion**: Failed conversions, mixed types, unparseable values
- **Statistical**: Simple outliers (> 3 std dev), negative revenues, zero concentrations
- **Domain-specific**: Multi-currency mixing, fiscal period gaps, customer ID format variance

### Flag vs Fix Policy
- **Auto-fix**: Header normalization, type coercion, currency parsing
- **Flag only**: Negative revenues, high null rates, outliers
- **Surface in**: `schema.json` warnings, `lineage.json` steps, optional LLM insights

## 7. Auditability

### Schema Registry
`schema.json` captures column-level metadata:
- Original vs normalized names
- Detected dtypes and roles
- Cardinality and null rates
- Applied transformations

### Lineage Tracking
`lineage.json` maintains provenance ledger:
```json
{
  "steps": [
    {
      "operation": "normalize",
      "timestamp": "2024-01-15T10:30:00Z",
      "transformations": ["currency_parsing", "header_cleanup"],
      "warnings": ["negative_revenue_detected"]
    }
  ]
}
```

### Future State
- OpenLineage: Standardized lineage events
- Marquez: Centralized metadata catalog
- DataHub/Amundsen: Discovery and governance layers

LLM usage is advisory only:
- Prompts/responses are persisted per dataset for audit
- Model is instructed to reference computed metrics only (no recomputation)
- The system operates without any LLM keys (insights disabled)

## 8. Extension Path

### Storage Evolution
**Current**: Local filesystem → **Next**: S3/GCS via fsspec
- Keep Parquet as interchange format
- No code changes, just config (`s3://bucket/path`)

### Compute Scaling
**Current**: Pandas (simplicity) → **Next**: Polars/DuckDB (performance)
- Same Parquet contracts
- 10-100x speedup for large datasets
- SQL pushdown capabilities

### Registry Migration
**Current**: JSON files → **Optional**: SQLite → **Production**: Postgres
- Start simple, migrate when needed
- Schema versioning and change management
- Multi-tenant dataset isolation

### Out of Scope (Future)
- **Dimensional modeling**: Star/snowflake schemas
- **FX conversion**: Requires rate tables and date alignment
- **ML-based anomaly detection**: Beyond deterministic rules
- **Real-time streaming**: Batch-first, stream later

---

## 9. Mapping to This POC's API

End-to-end flow with this repository:
1) Upload
```bash
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@sample.xlsx"
```
→ `{"dataset_id": "ds_..."}`

2) Inspect Schema
```bash
curl "http://localhost:8000/api/v1/schema/ds_..."
```
→ Returns `columns`, `period_grain`, `period_grain_candidates`, `time_candidates`, and `warnings`.

3) Run Concentration Analysis
```bash
curl -X POST "http://localhost:8000/api/v1/analyze/ds_.../concentration" \
  -H "Content-Type: application/json" \
  -d '{
    "group_by": "customer",
    "value": "revenue",
    "time": "period_key",
    "thresholds": [10,20,50]
  }'
```

4) Download Artifacts
```bash
curl "http://localhost:8000/api/v1/download/ds_.../concentration.csv"  --output results.csv
curl "http://localhost:8000/api/v1/download/ds_.../concentration.xlsx" --output results.xlsx
```

5) Optional Insights (advisory)
```bash
curl "http://localhost:8000/api/v1/insights/ds_..."
```

---

## 10. Worked Example (Mini)

Input table (simplified):
```
customer | date       | revenue
ACME     | 2024-01-10 | $2,345.67
BETA     | 2024-01-20 | 2,001
GAMMA    | 2024-01-05 | (1,000)   # negative (flag)
DELTA    | 2024-01-12 | 1.2k
```

Normalization:
- revenue → [2345.67, 2001.00, -1000.00, 1200.00]
- date → derive year=2024, month=01 → period_key=2024-M01
- Warnings: ["negative revenue detected for 'revenue'"]

Concentration (2024-M01):
- total = 4546.67
- sorted values: [2345.67, 2001.00, 1200.00, -1000.00]
- top_10: include entities while cumsum ≤ 454.667 → count 1 (ACME)
- top_20: cumsum ≤ 909.334 → count 1 (ACME)
- top_50: cumsum ≤ 2273.335 → count 2 (ACME+BETA)

Artifacts:
- Parquet: `storage/datasets/ds_.../normalized.parquet`
- JSON/CSV/XLSX under `.../analyses/`
- `schema.json` + `lineage.json` with full provenance

---

## 11. Analyst Checklist
- [ ] Headers normalized and unique
- [ ] Currency/percent parsed; negatives policy applied
- [ ] Time grain chosen; period_key correct; warnings reviewed
- [ ] Concentration thresholds and semantics verified
- [ ] Exports mirror JSON; Excel includes Data sheet
- [ ] Lineage and schema artifacts saved

---

*This POC establishes deterministic, auditable patterns that scale from prototype to production. Every transformation is traceable, every result reproducible, and every anomaly documented—building trust through transparency.*