# Keye POC - Data Analysis Pipeline

A proof-of-concept data analysis pipeline that ingests Excel/CSV files, performs concentration analysis, and generates AI-powered insights with full auditability.

## Features

- **Data Ingestion**: Upload Excel and CSV files
- **Automatic Schema Detection**: Intelligent type inference with LLM enhancement
- **Data Normalization**: Generic and domain-specific cleaning rules
- **Time Dimension Handling**: Composite time periods (year/month/quarter)
- **Concentration Analysis**: Deterministic analysis with configurable thresholds (1-100%)
- **AI Insights**: LLM-generated business insights and recommendations
- **Full Auditability**: Complete lineage tracking and reproducible results
- **Multiple Export Formats**: JSON, CSV, and Excel outputs

## Quick Start

### Using Docker (Recommended)

```bash
# Build and run with Docker Compose
docker-compose up --build

# API will be available at http://localhost:8000
```

### Local Development

```bash
# Create virtual environment with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt

# Run the API
uvicorn api.main:app --reload

# API will be available at http://localhost:8000
```

## API Documentation

Once running, visit:
- Interactive API docs: http://localhost:8000/docs
- Alternative API docs: http://localhost:8000/redoc
- Health check: http://localhost:8000/health

## API Endpoints

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/upload` | POST | Upload Excel/CSV file |
| `/api/v1/schema/{dataset_id}` | GET | Get detected schema |
| `/api/v1/analyze/{dataset_id}/concentration` | POST | Run concentration analysis |
| `/api/v1/insights/{dataset_id}` | GET | Get AI insights |
| `/api/v1/download/{dataset_id}/concentration.csv` | GET | Download CSV results |
| `/api/v1/download/{dataset_id}/concentration.xlsx` | GET | Download Excel results |
| `/api/v1/lineage/{dataset_id}` | GET | View audit trail |

### Example Usage

```bash
# Upload a file with request tracking
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -H "X-Request-ID: upload-001" \
  -H "X-API-Key: dev-key" \
  -F "file=@sample_data.xlsx"

# Response: {"dataset_id": "ds_abc123..."}

# Get schema
curl "http://localhost:8000/api/v1/schema/ds_abc123" \
  -H "X-API-Key: dev-key"

# Run concentration analysis (dynamic thresholds supported)
curl -X POST "http://localhost:8000/api/v1/analyze/ds_abc123/concentration" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-key" \
  -H "X-Request-ID: analyze-001" \
  -d '{
    "group_by": "customer",
    "value": "revenue",
    "thresholds": [5, 15, 50, 100]
  }'

# Get insights
curl "http://localhost:8000/api/v1/insights/ds_abc123" \
  -H "X-API-Key: dev-key"

# Get audit trail
curl "http://localhost:8000/api/v1/lineage/ds_abc123" \
  -H "X-API-Key: dev-key"

# Download results
curl "http://localhost:8000/api/v1/download/ds_abc123/concentration.xlsx" \
  -H "X-API-Key: dev-key" \
  --output results.xlsx

# Check API health
curl "http://localhost:8000/healthz"
```

## Architecture

### Project Structure

```
keye-poc/
├── api/               # FastAPI application
│   └── v1/           # API v1 routes and models
├── core/             # Core business logic
│   ├── deterministic/ # Deterministic calculations
│   └── llm/          # LLM integrations
├── services/         # Service layer
├── storage/          # Data storage
└── tests/            # Test suite
```
### Key Design Decisions

1. **Canonical Storage**: Parquet format for efficient, typed data storage
2. **Audit Trail**: Complete lineage tracking in JSON format
3. **Deterministic Core**: All calculations are deterministic and reproducible
4. **LLM Advisory Only**: AI provides insights but never computes metrics
5. **Graceful Degradation**: System works without time dimensions or LLM

## Configuration

Create a `.env` file in the project root:

```env
# LLM Configuration (optional)
USE_LLM=true
LLM_PROVIDER=openai
OPENAI_API_KEY=your-api-key-here

# Analysis Settings
DEFAULT_THRESHOLDS=[10,20,50]
MAX_FILE_SIZE_MB=100

# Development
DEBUG=false
```

## Testing

### Running Tests

```bash
# Run all tests with uv
uv run pytest

# Run with coverage
uv run pytest --cov=services --cov-report=term-missing

# Run specific test suites
uv run pytest tests/unit/          # Unit tests only
uv run pytest tests/integration/   # Integration tests only

# Run specific test file
uv run pytest tests/unit/test_registry.py
```

### Test Data Generation

The project includes a powerful test fixture generator that creates optimized samples from production Excel files:

```bash
# Generate test fixtures from your Excel file
uv run python scripts/generate_test_fixtures.py path/to/your/excel/file.xlsx

# This creates multiple test samples:
# - sample_20pct: 20% random sample for integration testing
# - sample_small: 5% sample for quick unit tests  
# - sample_time_balanced: All time periods with equal representation
# - sample_top_entities: Top 10 entities with all their data
# - sample_edge_cases: Edge cases (nulls, extremes, special chars)
# - sample_stratified: 15% sample maintaining data distribution
```

Test fixtures are saved in both Excel and CSV formats in `tests/fixtures/sample_data/`.

### Test Coverage

Current test coverage: **100%** for services layer (165/165 statements)
- 13 tests for DatasetRegistry
- 16 tests for StorageService  
- 13 tests for ExportService

For more details on testing strategy and fixture usage, see [docs/testing.md](docs/testing.md).

## Deployment Considerations

### Production Readiness

- Add authentication/authorization
- Configure CORS appropriately
- Set up proper logging
- Add rate limiting
- Configure SSL/TLS

### Scaling Path

- **Storage**: Migrate from filesystem to S3/Azure Blob
- **Database**: Add PostgreSQL for metadata
- **Queue**: Add Celery/Redis for async processing
- **Monitoring**: Integrate Prometheus/Grafana

## Tooling and AI Usage

I used Cursor/Claude Code to assist with writing code, tests, scaffolding, and documentation under my supervision. I reviewed and edited all generated code and take responsibility for it.

- All numerical results come from deterministic code with unit tests
- AI is used only for optional schema descriptions and narrative insights
- Prompts/responses are persisted per dataset for full transparency
- The system runs without LLM calls if API keys are unset

## License

This is a proof-of-concept project for evaluation purposes.

## Contact

For questions or issues, please contact the development team.