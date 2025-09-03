# Testing Documentation

## Overview

The Keye POC project uses a comprehensive testing strategy combining unit tests, integration tests, and real-world data fixtures to ensure reliability and correctness of the data analysis pipeline.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── fixtures/                # Test data files
│   ├── sample_data/        # Generated test samples
│   ├── real_data/          # Original files (optional)
│   ├── metadata.json       # Fixture generation metadata
│   └── README.md           # Fixture documentation
├── unit/                    # Unit tests (isolated components)
│   ├── test_registry.py    # DatasetRegistry tests
│   ├── test_storage.py     # StorageService tests
│   └── test_exporters.py   # ExportService tests
└── integration/             # Integration tests (full pipeline)
    └── test_real_data_pipeline.py
```

## Test Data Management

### Test Fixture Generator

The project includes a sophisticated test fixture generator (`scripts/generate_test_fixtures.py`) that creates optimized test data from production Excel files.

#### Usage

```bash
# Basic usage - generates all fixture types
uv run python scripts/generate_test_fixtures.py path/to/your/excel/file.xlsx

# Custom output directory
uv run python scripts/generate_test_fixtures.py file.xlsx --output-dir custom/path

# Different random seed for alternative samples
uv run python scripts/generate_test_fixtures.py file.xlsx --seed 999
```

#### Generated Fixtures

| Fixture Name | Size | Rows | Purpose |
|-------------|------|------|---------|
| **sample_20pct** | ~200KB | 20% of original | General integration testing |
| **sample_small** | ~50KB | 5% of original | Quick unit tests |
| **sample_time_balanced** | ~15KB | 10 per period | Time series testing |
| **sample_top_entities** | ~150KB | Top 10 entities | Concentration analysis |
| **sample_edge_cases** | ~6KB | ~10 rows | Error handling, edge cases |
| **sample_stratified** | ~130KB | 15% stratified | Statistical testing |

Each fixture is saved in both **Excel (.xlsx)** and **CSV (.csv)** formats for comprehensive format testing.

### How the Generator Works

1. **Column Detection**
   - Automatically identifies time columns (dates, periods)
   - Detects categorical columns (customer, product, etc.)
   - Identifies numeric value columns (revenue, quantity)

2. **Sampling Strategies**
   - **Random**: Pure random sampling for general testing
   - **Time-balanced**: Equal rows from each time period
   - **Top entities**: All data for highest-value entities
   - **Edge cases**: Nulls, extremes, special characters
   - **Stratified**: Maintains proportional representation

3. **Dual Format Output**
   - Excel format preserves original structure and types
   - CSV format for lightweight, universal compatibility
   - Both formats tested to ensure consistency

## Test Types

### Unit Tests

Located in `tests/unit/`, these tests verify individual components in isolation.

#### Registry Tests (`test_registry.py`)
- Dataset creation and folder structure
- Lineage tracking and step recording
- Schema management
- LLM artifact storage
- Error handling for missing datasets

```python
def test_create_dataset(registry, mock_datasets_path):
    dataset_id = registry.create_dataset("test.xlsx")
    assert dataset_id.startswith("ds_")
    assert len(dataset_id) == 15
```

#### Storage Tests (`test_storage.py`)
- Parquet round-trip integrity
- CSV/Excel read/write operations
- Multi-sheet Excel handling
- SHA256 checksum calculation
- Special character and null handling
- Empty DataFrame edge cases

```python
def test_parquet_round_trip(temp_dir, sample_df):
    checksum = StorageService.write_parquet(sample_df, parquet_path)
    read_df = StorageService.read_parquet(parquet_path)
    pd.testing.assert_frame_equal(sample_df, read_df)
```

#### Exporter Tests (`test_exporters.py`)
- CSV export structure validation
- Excel multi-sheet generation
- JSON serialization
- Threshold data accuracy
- Empty result handling

```python
def test_export_concentration_excel_sheets_present(temp_dir, results):
    ExportService.export_concentration_excel(results, excel_path)
    workbook = load_workbook(excel_path)
    assert "Summary" in workbook.sheetnames
    assert "Details" in workbook.sheetnames
    assert "Parameters" in workbook.sheetnames
```

### Integration Tests

Located in `tests/integration/`, these tests verify the complete pipeline with real data.

#### Full Pipeline Testing
```python
def test_full_pipeline_with_20pct_sample(sample_20pct_excel, registry):
    # Create dataset
    dataset_id = registry.create_dataset(sample_20pct_excel.name)
    
    # Process file
    df = StorageService.read_excel(sample_20pct_excel)
    StorageService.write_parquet(df, normalized_path)
    
    # Run analysis
    results = analyze_concentration(df)
    
    # Export results
    ExportService.export_concentration_excel(results, export_path)
    
    # Verify lineage
    lineage = registry.get_lineage(dataset_id)
    assert len(lineage["steps"]) >= 2
```

#### Performance Testing
```python
def test_large_file_performance(sample_20pct_excel):
    start = time.time()
    df = StorageService.read_excel(sample_20pct_excel)
    excel_read_time = time.time() - start
    
    # Parquet should be faster
    assert parquet_read_time < excel_read_time * 2
```

## Test Fixtures

### Programmatic Fixtures (`conftest.py`)

Dynamic fixtures created at runtime:

```python
@pytest.fixture
def sample_df():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        "Company": ["ACME Corp", "Beta Inc"],
        "Revenue": [1000000, 750000]
    })

@pytest.fixture
def temp_dir():
    """Create isolated temp directory."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)
```

### File-based Fixtures

Real data files for integration testing:

```python
@pytest.fixture
def sample_20pct_excel():
    """Path to 20% sampled Excel file."""
    path = Path(__file__).parent / "fixtures/sample_data/sample_20pct.xlsx"
    if not path.exists():
        pytest.skip("Run generate_test_fixtures.py first")
    return path
```

## Running Tests

### Basic Commands

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/unit/test_registry.py

# Run specific test
uv run pytest tests/unit/test_registry.py::test_create_dataset

# Run tests matching pattern
uv run pytest -k "parquet"
```

### Coverage Analysis

```bash
# Run with coverage report
uv run pytest --cov=services --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=services --cov-report=html

# Coverage for specific modules
uv run pytest --cov=services.registry --cov=services.storage
```

Current coverage: **100%** across all service modules (165/165 statements)

### Test Markers

```bash
# Run only quick tests (if marked)
uv run pytest -m "not slow"

# Run integration tests only
uv run pytest tests/integration/

# Skip integration tests
uv run pytest tests/unit/
```

## Best Practices

### 1. Test Isolation
- Each test should be independent
- Use fixtures for setup/teardown
- Mock external dependencies
- Use temp directories for file operations

### 2. Fixture Selection
- Use `sample_small` for quick unit tests
- Use `sample_20pct` for integration tests
- Use `sample_edge_cases` for error handling
- Use `sample_time_balanced` for time series logic

### 3. Assertion Guidelines
```python
# Good - specific assertion
assert len(df) == 883
assert dataset_id.startswith("ds_")

# Better - with error message
assert len(df) == 883, f"Expected 883 rows, got {len(df)}"

# Best - using pandas testing utilities
pd.testing.assert_frame_equal(expected_df, actual_df)
```

### 4. Performance Testing
- Test with realistic data sizes
- Use the 20% sample for performance benchmarks
- Compare Excel vs Parquet read times
- Monitor memory usage for large files

## Continuous Integration

### GitHub Actions Configuration

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.12'
      - name: Install uv
        run: pip install uv
      - name: Install dependencies
        run: uv pip install -r requirements.txt
      - name: Run tests
        run: uv run pytest --cov=services
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: uv run pytest tests/unit/
        language: system
        pass_filenames: false
        always_run: true
```

## Troubleshooting

### Common Issues

#### "Fixture file not found"
```bash
# Generate fixtures first
uv run python scripts/generate_test_fixtures.py your_excel_file.xlsx
```

#### Import errors
```bash
# Ensure all dependencies are installed
uv pip install -r requirements.txt
```

#### Slow tests
- Use `sample_small` instead of `sample_20pct`
- Run unit tests only: `uv run pytest tests/unit/`
- Use pytest-xdist for parallel execution

### Testing Notes (Rate Limiting)
- The rate limiter enforces 60 requests/minute per IP and path. This design reduces cross-endpoint interference during tests.
- The `/healthz` endpoint uses a shorter internal decay window to avoid lingering throttling during readiness bursts, ensuring stable CI runs.
- For background and rationale, see `docs/decisions/0005_rate-limiting-keying-and-testability.md`.

#### Coverage gaps
```bash
# Find uncovered lines
uv run pytest --cov=services --cov-report=term-missing
```

## Test Data Examples

### Sample Financial Data Structure
```
# From sample_small.csv
Year  Month  Customer Code  Industry              Revenue    Gross Profit
2023  1      Customer 118   Consulting            8.95       3.03
2021  4      Customer 185   Pharmaceuticals       3893.51    886.91
2020  5      Customer 585   Apparel               0.00       0.00
```

### Edge Cases in Fixtures
- Negative revenues (intercompany eliminations)
- Zero values
- Null/missing data
- Special characters in names
- Extreme values (min/max)
- Unicode characters

## Future Enhancements

### Planned Improvements
1. **Property-based testing** with Hypothesis
2. **Mutation testing** to verify test effectiveness
3. **Load testing** for API endpoints
4. **Snapshot testing** for complex outputs
5. **Contract testing** for API compatibility
6. **Fuzz testing** for edge case discovery

### Additional Fixture Types
- Malformed files for error testing
- Large files for stress testing
- Multi-sheet Excel with complex formulas
- Time series with gaps and irregularities
- International data with various encodings

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Pandas Testing Guide](https://pandas.pydata.org/docs/reference/testing.html)
- [Test Fixtures README](../tests/fixtures/README.md)
- [Generate Test Fixtures Script](../scripts/generate_test_fixtures.py)