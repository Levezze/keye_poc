## API v1 Overview

### Authentication
- Optional API key via header: `X-API-Key: <key>`
- All requests support `X-Request-ID` header for tracking

### Endpoints

| Method | Path | Description |
|---|---|---|
| POST | /api/v1/upload | Upload Excel/CSV file |
| GET | /api/v1/schema/{dataset_id} | Get detected schema |
| POST | /api/v1/analyze/{dataset_id}/concentration | Run concentration analysis |
| GET | /api/v1/download/{dataset_id}/concentration.csv | Download CSV results |
| GET | /api/v1/download/{dataset_id}/concentration.xlsx | Download Excel results |
| GET | /api/v1/insights/{dataset_id} | Get AI insights |
| GET | /api/v1/lineage/{dataset_id} | Get dataset audit trail |
| GET | /healthz | Enhanced health check |

### Rate Limiting
- 60 requests per minute per IP address
- Returns 429 status with `Retry-After` header when exceeded

### Error Response Format
All errors return a standardized JSON structure:

```json
{
  "error": "ErrorType",
  "message": "Human-readable description",
  "details": {
    "field": "additional context"
  },
  "request_id": "uuid"
}
```

Error types: `ValidationError`, `NotFound`, `Conflict`, `RateLimited`, `PayloadTooLarge`, `InternalError`, `Unauthorized`

### Models

ConcentrationRequest
```json
{
  "group_by": "string",
  "value": "string",
  "thresholds": [10, 20, 50, 100]
}
```

- `thresholds`: Array of concentration percentages (1-100). Values are automatically sorted and deduplicated.
- Default: `[10, 20, 50]`
- Maximum: 10 thresholds per request
- Range: 1% to 100% inclusive

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

Upload with Request Tracking
```bash
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "Content-Type: multipart/form-data" \
  -H "X-Request-ID: upload-001" \
  -H "X-API-Key: dev-key" \
  -F "file=@customer_data.xlsx"
```

Response:
```json
{
  "dataset_id": "ds_abc123456789",
  "status": "completed",
  "message": "Successfully processed 1000 rows with 5 columns",
  "rows_processed": 1000,
  "columns_processed": 5
}
```

Get Schema
```bash
curl "http://localhost:8000/api/v1/schema/ds_abc123456789" \
  -H "X-API-Key: dev-key" \
  -H "X-Request-ID: schema-001"
```

Analyze (custom thresholds)
```bash
curl -X POST "http://localhost:8000/api/v1/analyze/ds_abc123456789/concentration" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key" \
  -H "X-Request-ID: analyze-001" \
  -d '{
    "group_by": "customer_name",
    "value": "revenue",
    "thresholds": [5, 25, 75]
  }'
```

Get Lineage
```bash
curl "http://localhost:8000/api/v1/lineage/ds_abc123456789" \
  -H "X-API-Key: dev-key" \
  -H "X-Request-ID: lineage-001"
```

Download CSV
```bash
curl "http://localhost:8000/api/v1/download/ds_abc123456789/concentration.csv" \
  -H "X-API-Key: dev-key" \
  -o concentration.csv
```

Health Check
```bash
curl "http://localhost:8000/healthz"
```

Response:
```json
{
  "status": "healthy",
  "service": "keye-poc-api",
  "checks": {
    "storage_directory": {
      "status": "healthy",
      "path": "storage/datasets",
      "writable": true
    },
    "directory_creation": {
      "status": "healthy"
    }
  }
}
```

### Threshold Semantics
- Deterministic ranking: ORDER BY value DESC, then group_by ASC
- For threshold X, include entities with cumulative percentage ≤ X%
- If none qualify, include at least the top 1 entity

### CSV/Excel Formats
- CSV columns: period, threshold, count, value, pct_of_total
- Excel sheets: Summary, Top_Entities, Parameters

### Error Examples

400 - Bad Request (Invalid Dataset ID)
```bash
curl "http://localhost:8000/api/v1/schema/invalid-format" \
  -H "X-API-Key: dev-key" \
  -H "X-Request-ID: error-test"
```

Response:
```json
{
  "error": "ValidationError",
  "message": "Invalid dataset ID format",
  "details": null,
  "request_id": "error-test"
}
```

404 - Not Found
```bash
curl "http://localhost:8000/api/v1/schema/ds_000000000000" \
  -H "X-API-Key: dev-key"
```

Response:
```json
{
  "error": "NotFound", 
  "message": "Dataset ds_000000000000 not found",
  "details": null,
  "request_id": "generated-uuid"
}
```

429 - Rate Limited
```json
{
  "error": "RateLimited",
  "message": "Rate limit exceeded: 60 requests per minute",
  "details": {
    "limit": 60,
    "window": "1 minute"
  },
  "request_id": "rate-limit-test"
}
```

413 - Payload Too Large
```json
{
  "error": "PayloadTooLarge",
  "message": "File exceeds 25MB limit",
  "details": {
    "limit_mb": 25
  },
  "request_id": "upload-large"
}
```

### Status Codes
- 200: Success
- 400: Bad request (invalid parameters, dataset ID format)
- 401: Unauthorized (missing/invalid API key)
- 404: Dataset or analysis not found
- 413: File too large (>25MB)
- 422: Validation error (invalid request body)
- 429: Rate limit exceeded (>60/min)
- 500: Server error
