# Concentration Analysis API Guide

Complete guide for using the concentration analysis endpoints with examples and troubleshooting.

## API Overview

The concentration analysis feature analyzes how value is distributed across entities, computing thresholds like "top 10%" based on cumulative percentage contributions.

**Key Concepts:**
- **Entity**: The dimension to group by (e.g., customers, products, regions)
- **Value**: The metric to analyze (e.g., revenue, sales, units)
- **Thresholds**: Percentage cutoffs (e.g., [10, 20, 50] for top 10%, 20%, 50%)
- **Periods**: Optional time-based grouping for temporal analysis

## Complete Workflow

### 1. Upload Data

```bash
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@customer_data.xlsx"
```

**Response:**
```json
{
  "dataset_id": "ds_a1b2c3d4e5f6",
  "status": "completed", 
  "message": "Successfully processed 1000 rows with 5 columns",
  "rows_processed": 1000,
  "columns_processed": 5
}
```

### 2. Check Schema

```bash
curl "http://localhost:8000/api/v1/schema/ds_a1b2c3d4e5f6"
```

**Response:**
```json
{
  "dataset_id": "ds_a1b2c3d4e5f6",
  "columns": [
    {
      "name": "customer_name",
      "original_name": "Customer Name", 
      "dtype": "object",
      "role": "categorical",
      "cardinality": 500,
      "null_rate": 0.0
    },
    {
      "name": "revenue",
      "original_name": "Revenue ($)",
      "dtype": "float64", 
      "role": "numeric",
      "cardinality": 1000,
      "null_rate": 0.02
    },
    {
      "name": "date",
      "original_name": "Date",
      "dtype": "datetime64[ns]",
      "role": "datetime", 
      "cardinality": 365,
      "null_rate": 0.0
    }
  ],
  "period_grain": "year-month",
  "period_grain_candidates": ["date", "year-month", "year"],
  "time_candidates": ["date"],
  "warnings": [],
  "notes": ["Detected temporal dimension with monthly granularity"]
}
```

### 3. Run Concentration Analysis

**Basic Analysis:**
```bash
curl -X POST "http://localhost:8000/api/v1/analyze/ds_a1b2c3d4e5f6/concentration" \
  -H "Content-Type: application/json" \
  -d '{
    "group_by": "customer_name",
    "value": "revenue"
  }'
```

**Custom Thresholds:**
```bash  
curl -X POST "http://localhost:8000/api/v1/analyze/ds_a1b2c3d4e5f6/concentration" \
  -H "Content-Type: application/json" \
  -d '{
    "group_by": "customer_name", 
    "value": "revenue",
    "thresholds": [5, 15, 80]
  }'
```

**Response:**
```json
{
  "dataset_id": "ds_a1b2c3d4e5f6",
  "period_grain": "year-month",
  "thresholds": [10, 20, 50],
  "warnings": ["concentration_calculation_TOTAL: completed"],
  "by_period": [
    {
      "period": "2023-01",
      "total": 150000.0,
      "top_10": {
        "count": 3,
        "value": 15000.0,
        "pct_of_total": 10.0
      },
      "top_20": {
        "count": 8,
        "value": 30000.0, 
        "pct_of_total": 20.0
      },
      "top_50": {
        "count": 25,
        "value": 75000.0,
        "pct_of_total": 50.0
      },
      "head": [
        {
          "customer_name": "Enterprise Corp",
          "revenue": 5000.0,
          "cumsum": 5000.0,
          "cumulative_pct": 3.33
        },
        {
          "customer_name": "Big Client Ltd", 
          "revenue": 4500.0,
          "cumsum": 9500.0,
          "cumulative_pct": 6.33
        }
      ]
    }
  ],
  "totals": {
    "period": "TOTAL",
    "total_entities": 500,
    "total_value": 2500000.0,
    "concentration": {
      "top_10": {
        "count": 15,
        "value": 250000.0,
        "pct_of_total": 10.0
      },
      "top_20": {
        "count": 35, 
        "value": 500000.0,
        "pct_of_total": 20.0
      },
      "top_50": {
        "count": 125,
        "value": 1250000.0,
        "pct_of_total": 50.0
      }
    }
  },
  "export_links": {
    "csv": "/api/v1/download/ds_a1b2c3d4e5f6/concentration.csv",
    "xlsx": "/api/v1/download/ds_a1b2c3d4e5f6/concentration.xlsx"
  }
}
```

### 4. Download Results

**CSV Export:**
```bash
curl "http://localhost:8000/api/v1/download/ds_a1b2c3d4e5f6/concentration.csv" \
  --output concentration_results.csv
```

**Excel Export:**
```bash
curl "http://localhost:8000/api/v1/download/ds_a1b2c3d4e5f6/concentration.xlsx" \
  --output concentration_results.xlsx
```

## Algorithm Details

### Threshold Semantics

**"≤ X% with at least 1 entity" rule:**

- For threshold 10%, include entities whose **cumulative percentage ≤ 10%**
- If no entities qualify (first entity > 10%), include **at least the top 1 entity**
- Deterministic tie-breaking: ORDER BY value DESC, then entity name ASC

**Example:**
```
Entities: CustomerA($1000), CustomerB($500), CustomerC($300)
Total: $1800
Cumulative: CustomerA(55.6%), CustomerB(83.3%), CustomerC(100%)

Threshold 10%: CustomerA only (55.6% > 10%, but at least 1 entity)
Threshold 50%: CustomerA only (55.6% > 50%, but at least 1 entity) 
Threshold 90%: CustomerA + CustomerB (83.3% ≤ 90%)
```

### Period Handling

**Single Period (no time dimension):**
- All data analyzed as one group
- Result appears in `totals` section with period "TOTAL"

**Multi-Period (time dimension detected):**
- Each period analyzed separately
- Results appear in `by_period` array
- Overall aggregate appears in `totals` with period "TOTAL"

## Common Use Cases

### 1. Customer Revenue Concentration
```json
{
  "group_by": "customer_name",
  "value": "revenue", 
  "thresholds": [10, 20, 50]
}
```
**Insight:** "Top 10% of customers contribute 60% of revenue"

### 2. Product Sales Analysis
```json
{
  "group_by": "product_sku",
  "value": "units_sold",
  "thresholds": [5, 15, 80]
}  
```
**Insight:** "Top 5% of products account for 40% of unit sales"

### 3. Geographic Revenue Distribution
```json
{
  "group_by": "region",
  "value": "total_sales",
  "thresholds": [20, 50, 80]
}
```
**Insight:** "Top 20% of regions generate 70% of sales"

## Export Formats

### CSV Structure
```csv
period,threshold,count,value,pct_of_total
2023-01,10,3,15000.0,10.0
2023-01,20,8,30000.0,20.0  
2023-01,50,25,75000.0,50.0
TOTAL,10,15,250000.0,10.0
TOTAL,20,35,500000.0,20.0
TOTAL,50,125,1250000.0,50.0
GroupBy,customer_name
```

### Excel Sheets

**Summary Sheet:**
- One row per period with all threshold metrics
- Columns: period, total, top_10_count, top_10_value, top_10_pct, etc.

**Details Sheet:**
- Head sample data (top entities per period) 
- Columns: period, entity, value, cumsum, cumulative_pct

**Parameters Sheet:**
- Analysis configuration
- Columns: Parameter, Value (Group By, Value Column, Time Column, Thresholds)

## Troubleshooting Guide

### Common Issues

#### 1. "Total value is non-positive; cannot compute concentration"

**Cause:** All values sum to zero or negative
```json
{
  "dataset_id": "ds_123",
  "totals": {
    "error": "Total value is non-positive; cannot compute concentration"
  }
}
```

**Solutions:**
- Check for data quality issues (all zeros, negative values)
- Verify correct value column selection
- Filter out invalid records before analysis

#### 2. "Column 'customer_id' not found in dataset"

**Cause:** Specified group_by or value column doesn't exist

**Solutions:**
- Check schema endpoint first: `GET /api/v1/schema/{dataset_id}`
- Use exact column names from schema (normalized, lowercase, snake_case)
- Verify dataset was processed successfully

#### 3. No periods in `by_period` array

**Cause:** Single-period analysis (no time dimension detected)

**Expected Behavior:**
- Results appear in `totals` section only
- `by_period` array will be empty
- `period_grain` will be "none"

**If you expected multi-period:**
- Check if time columns were detected in schema
- Verify time data format (dates, year-month, etc.)
- Time detection requires recognizable column names or formats

#### 4. Unexpected threshold results

**Example Issue:** "Top 10% includes 50% of entities"

**Explanation:** This is correct behavior with the "≤ X%" algorithm

**Understanding:**
```
10 entities with values: [100, 100, 100, 100, 100, 10, 10, 10, 10, 10]
Total: 550
Top 10%: First 5 entities (500/550 = 90.9% of value, but only 9.1% cumulative)
```

**The algorithm finds entities where cumulative % ≤ threshold, not entities that contribute threshold% of value.**

#### 5. Performance issues with large datasets

**Symptoms:**
- Slow response times
- Timeout errors
- High memory usage

**Solutions:**
- Current limit: 10,000 entities tested
- For larger datasets: consider data sampling or pre-aggregation
- Use more specific time periods to reduce data volume

#### 6. Inconsistent results across runs

**This should not happen** - the algorithm is deterministic

**If you see this:**
1. Verify same exact data and parameters
2. Check for data modifications between runs
3. Report as potential bug

### Error Response Format

All errors return HTTP status codes with JSON details:

```json
{
  "detail": "Column 'invalid_column' not found in dataset" 
}
```

**Common Status Codes:**
- `400`: Bad request (invalid parameters, missing columns)
- `404`: Dataset not found or analysis not run
- `413`: File too large
- `422`: Validation error (invalid JSON, parameter types)
- `500`: Internal server error

### Performance Expectations

**Typical Performance:**
- 1,000 entities: < 1 second
- 10,000 entities: < 5 seconds  
- 100,000 entities: Not tested (may require optimization)

**Memory Usage:**
- Proportional to dataset size
- Peak usage during sorting and cumulative calculation
- Head samples limited to 10 items per period to control payload size

### Best Practices

1. **Always check schema first** before running analysis
2. **Use appropriate thresholds** (standard: [10,20,50]; custom as needed)
3. **Validate data quality** (check for negatives, nulls, outliers)
4. **Start with small datasets** to validate approach
5. **Cache results** if running same analysis repeatedly
6. **Use time periods** to reduce computational load for large datasets

## API Authentication

If API key authentication is configured:

```bash
curl -H "X-API-Key: your-api-key-here" \
  "http://localhost:8000/api/v1/schema/ds_123"
```

Without API key (if not configured):
```bash
curl "http://localhost:8000/api/v1/schema/ds_123"
```