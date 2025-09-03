## API v1 Overview

### Authentication
- Optional API key via header: `X-API-Key: <key>`

### Endpoints

| Method | Path | Description |
|---|---|---|
| POST | /api/v1/upload | Upload Excel/CSV file |
| GET | /api/v1/schema/{dataset_id} | Get detected schema |
| POST | /api/v1/analyze/{dataset_id}/concentration | Run concentration analysis |
| GET | /api/v1/download/{dataset_id}/concentration.csv | Download CSV results |
| GET | /api/v1/download/{dataset_id}/concentration.xlsx | Download Excel results |
| GET | /api/v1/insights/{dataset_id} | Get AI insights |

### Models

ConcentrationRequest
```json
{
  "group_by": "string",
  "value": "string",
  "thresholds": [10, 20, 50]
}
```

ConcentrationResponse (shape excerpt)
```json
{
  "dataset_id": "ds_...",
  "period_grain": "year_quarter|year_month|year|none",
  "thresholds": [10, 20, 50],
  "warnings": ["..."],
  "by_period": [
    {
      "period": "2023-Q1",
      "total": 2500000.0,
      "concentration": {
        "top_10": {"count": 1, "value": 1000000.0, "pct_of_total": 40.0},
        "top_20": {"count": 2, "value": 1750000.0, "pct_of_total": 70.0}
      },
      "head": [{"entity": "...", "value": 1000.0, "cumsum": 1000.0, "cumulative_pct": 1.0}]
    }
  ],
  "totals": {
    "period": "TOTAL",
    "total_entities": 500,
    "total_value": 2500000.0,
    "concentration": {"top_10": {"count": 15, "value": 250000.0, "pct_of_total": 10.0}}
  },
  "export_links": {"csv": "/api/v1/download/ds_.../concentration.csv", "xlsx": "/api/v1/download/ds_.../concentration.xlsx"}
}
```

### Examples

Upload
```bash
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@customer_data.xlsx"
```

Analyze (custom thresholds)
```bash
curl -X POST "http://localhost:8000/api/v1/analyze/ds_123/concentration" \
  -H "Content-Type: application/json" \
  -d '{
    "group_by": "customer_name",
    "value": "revenue",
    "thresholds": [5, 25, 75]
  }'
```

Download CSV
```bash
curl "http://localhost:8000/api/v1/download/ds_123/concentration.csv" -o concentration.csv
```

### Threshold Semantics
- Deterministic ranking: ORDER BY value DESC, then group_by ASC
- For threshold X, include entities with cumulative percentage ≤ X%
- If none qualify, include at least the top 1 entity

### CSV/Excel Formats
- CSV columns: period, threshold, count, value, pct_of_total
- Excel sheets: Summary, Top_Entities, Parameters

### Errors
- 400: Bad request (e.g., invalid columns, invalid thresholds)
- 404: Dataset or analysis not found
- 413: File too large
- 422: Validation error
- 500: Server error
