# ADR 0004: Deterministic Regex Normalization & Time Detection

Status: Proposed
Date: 2025-09-02

## Context
We need deterministic, auditable parsing of numeric, currency, percent, and time values across diverse Excel/CSV inputs. This ADR formalizes regex patterns, parsing order, locale handling, counters, and warnings to align with the POC’s guarantees and with `docs/financial_analysis.md` and `.dev/final_plan.md`.

## Decision
- Parsing order (per column, vectorized where possible):
  1) Trim/normalize whitespace (incl. NBSP `\u00A0` and NNBSP `\u202F`)
  2) Detect and extract sign (parentheses, ASCII `-`, Unicode `−`, trailing minus)
  3) Detect and strip currency symbol (prefix/suffix) and optional scale suffix
  4) Resolve thousands/decimal separators (US/EU rules; mixed flagged)
  5) Coerce to float
- Currency:
  - Symbols supported both prefix/suffix: `$ € £ ¥`; apostrophe and space thousands separators allowed (`1'234.50`, `1 234,50`).
  - Multi-currency: if >1 distinct symbol/code appears in a column, set `metadata.multi_currency=true` and warn; no FX conversion in POC.
- Scaling suffixes:
  - Supported: `k|K` (1e3), `m|M|mm` (1e6), `b|B|bn` (1e9). `mm` interpreted as “million” in finance context.
- Percent:
  - Value-level `%` → divide by 100; header hints `(percent|pct)` imply values in `[0,1]` are left as-is, `[1,100]` scaled to `[0,1]`; `0.5%` → `0.005`.
  - Schema marks `representation: "percent"`.
- Decimal conventions:
  - US example: `1,234.56`; EU example: `1.234,56`. NBSP and NNBSP treated as thousands separators.
  - Heuristic: if both `.` and `,` appear, infer by position (rightmost punctuation as decimal) or match common groupings; otherwise infer by trailing 1–2 decimals vs 3-digit groups. Mixed within a single column → warning (`decimal_convention: mixed`).
- Sign handling:
  - Parentheses negate; ASCII `-` and Unicode `−` normalized to ASCII; trailing minus supported (`1,234.50-`).
- Datetime:
  - Precedence: `date` > `year+month` > `year+quarter` > `year` > none.
  - Coercion via `pandas.to_datetime` with deterministic defaults (`dayfirst=False`, `errors='coerce'`); ambiguous/mixed locales → warning.
- Negative policy:
  - Flag negatives for `revenue` (do not allowlist).
  - Allow negatives for `gross_profit`, `net_income`, `cost`, `expense`, `adjustments`.
- Counters (schema `coercions` per column):
  - `currency_removed`, `parentheses_to_negative`, `scaling_applied`, `percent_normalized`, `datetime_parsed`, `boolean_coerced`, `failed_numeric`, `unicode_minus_normalized`.
- Warnings (examples):
  - `"Multi-currency data detected"`
  - `"Mixed decimal conventions within column 'X'"`
  - `"N values could not be coerced to numeric in 'X'"`
  - `"Unexpected negative values in column 'X'"`
  - `"Ambiguous date formats; defaulted to dayfirst=False"`

## Regex Patterns (reference)
```regex
# Currency (US-style, symbol prefix/suffix, optional scale)
^(?P<neg>\()?\s*(?P<cur>[\$€£¥])?\s*
(?P<num>(?:\d{1,3}(?:[,\s\u00A0\u202F]\d{3})+|\d+)(?:\.\d+)?|\d+(?:\.\d+)?)
\s*(?P<cur_suf>[\$€£¥])?\s*(?P<scale>k|K|m|M|mm|b|B|bn)?\s*(?(neg)\))$

# Currency (EU-style, symbol prefix/suffix, optional scale)
^(?P<neg>\()?\s*(?P<cur>[\$€£¥])?\s*
(?P<num>(?:\d{1,3}(?:[.\s\u00A0\u202F]\d{3})+|\d+)(?:,\d+)?|\d+(?:,\d+)?)
\s*(?P<cur_suf>[\$€£¥])?\s*(?P<scale>k|K|m|M|mm|b|B|bn)?\s*(?(neg)\))$

# Percent (value-level)
^\s*[+\-\u2212]?\d+(?:[.,]\d+)?\s*%\s*$

# Percent (header-level hint)
(?i)\b(percent|pct|percentage)\b

# Unicode minus detection
^\s*[\u2212-]

# Time column name hints (case-insensitive)
(?i)\b(date|dt|as_of|posting_date|transaction_date|year|month|quarter|fiscal_period)\b
# Quarter values
(?i)\bq([1-4])\b|\bquarter\b
# Year-only values
^(19|20)\d{2}$
# Year-month values (YYYY-MM or MM/YYYY)
^(?:\d{4}[-/]?(0[1-9]|1[0-2])|(?:0[1-9]|1[0-2])[-/](19|20)\d{2})$
```

## Rationale
- Ensures deterministic, auditable parsing across locales and messy spreadsheets.
- Aligns with `final_plan.md` and `docs/financial_analysis.md` goals without introducing runtime locale dependencies.

## Consequences
- Behavior is predictable and testable; mixed-locale inputs surface explicit warnings.
- Slightly more complex normalization logic; performance remains acceptable with vectorized operations.

## Alternatives Considered
- Rely solely on `to_numeric`/`to_datetime`: insufficient for symbols/scales/locales.
- Use locale/ICU parsing: heavier dependency, more config for POC.
- Third-party price parsers: helpful but reduce transparency in ADR/docs.

## Linkage
- `docs/financial_analysis.md`: add a short “Regex Reference” subsection linking to this ADR.
- `core/deterministic/normalization.py`: implements this order-of-operations and counters.

## Follow-ups
- Configurable locale hints (env/params).
- Extend currency detection to 3-letter codes (USD/EUR/GBP) when embedded in values.
- More unit tests for NBSP, apostrophes, trailing minus, and mixed-locale detection.
