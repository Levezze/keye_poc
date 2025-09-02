# Test Fixtures

This directory contains test data files used for testing the Keye POC application.

## Directory Structure

```
tests/fixtures/
├── sample_data/        # Generated sample fixtures
│   ├── *.xlsx         # Excel versions
│   └── *.csv          # CSV versions
├── real_data/         # Original full-size files (optional)
└── metadata.json      # Information about generated fixtures
```

## Generating Test Fixtures

Use the provided script to generate test fixtures from your real Excel file:

```bash
# Generate fixtures from your 1.2MB Excel file
uv run python scripts/generate_test_fixtures.py path/to/your/excel/file.xlsx

# With custom options
uv run python scripts/generate_test_fixtures.py path/to/file.xlsx \
    --output-dir tests/fixtures \
    --seed 42
```

## Generated Fixtures

The script creates multiple fixture variations, each in both Excel and CSV formats:

### 1. **sample_20pct** (20% Random Sample)
- **Purpose:** General integration testing
- **Size:** ~240KB
- **Use Case:** Testing full pipeline with realistic data volume

### 2. **sample_small** (5% Random Sample)
- **Purpose:** Quick unit tests
- **Size:** ~60KB
- **Use Case:** Fast test execution during development

### 3. **sample_time_balanced** (Time-Balanced Sample)
- **Purpose:** Testing time-series functionality
- **Size:** Varies
- **Use Case:** Ensures all time periods are represented with 10 rows each

### 4. **sample_top_entities** (Top 10 Entities)
- **Purpose:** Testing concentration analysis
- **Size:** Varies
- **Use Case:** Tests with known top performers, all their data included

### 5. **sample_edge_cases** (Edge Cases)
- **Purpose:** Testing error handling and data validation
- **Size:** Small
- **Includes:**
  - Rows with null values
  - Extreme values (min/max)
  - Special characters
  - Unicode characters

### 6. **sample_stratified** (15% Stratified Sample)
- **Purpose:** Maintaining data distribution
- **Size:** ~180KB
- **Use Case:** Statistical testing that requires representative distribution

## Using Fixtures in Tests

### Unit Tests
```python
def test_process_small_excel(sample_small_excel):
    """Test with small Excel fixture for quick execution."""
    df = StorageService.read_excel(sample_small_excel)
    assert len(df) > 0
    # Your test logic here
```

### Integration Tests
```python
def test_full_pipeline(sample_20pct_excel):
    """Test complete pipeline with 20% sample."""
    # Upload
    dataset_id = registry.create_dataset(sample_20pct_excel.name)
    
    # Process
    df = StorageService.read_excel(sample_20pct_excel)
    StorageService.write_parquet(df, f"storage/datasets/{dataset_id}/normalized.parquet")
    
    # Analyze
    results = analyze_concentration(df)
    assert results is not None
```

### Edge Case Testing
```python
def test_handle_nulls(sample_edge_cases_excel):
    """Test handling of null values and special characters."""
    df = StorageService.read_excel(sample_edge_cases_excel)
    # Test null handling
    assert df.isnull().any().any()  # Ensure nulls exist
    processed = normalize_data(df)
    # Verify proper null handling
```

### CSV vs Excel Testing
```python
def test_format_compatibility(sample_20pct_excel, sample_20pct_csv):
    """Test that both formats produce same results."""
    df_excel = StorageService.read_excel(sample_20pct_excel)
    df_csv = StorageService.read_csv(sample_20pct_csv)
    
    # DataFrames should be equivalent
    pd.testing.assert_frame_equal(df_excel, df_csv)
```

## Fixture Metadata

The `metadata.json` file contains information about:
- Source file used
- Generation timestamp
- Random seed (for reproducibility)
- Detected columns (time, group, value)
- Details about each generated fixture:
  - Number of rows and columns
  - File sizes
  - SHA256 checksums

## Best Practices

1. **Keep fixtures small:** Aim for fixtures under 500KB for fast test execution
2. **Use appropriate fixture:** Choose the smallest fixture that tests your scenario
3. **Version control:** Commit generated fixtures to git (they're small enough)
4. **Regenerate when needed:** Re-run the script when source data structure changes
5. **Document changes:** Update this README when adding new fixture types

## Adding Custom Fixtures

To add custom fixture types, modify `scripts/generate_test_fixtures.py`:

```python
def custom_sample(self) -> pd.DataFrame:
    """Create a custom sample for specific testing needs."""
    # Your sampling logic here
    return sampled_df
```

Then regenerate fixtures:
```bash
uv run python scripts/generate_test_fixtures.py your_file.xlsx
```

## CI/CD Integration

These fixtures are designed to work in CI/CD pipelines:
- Small file sizes for fast downloads
- Reproducible with seed parameter
- Both Excel and CSV formats for compatibility
- Metadata for validation

## Troubleshooting

### "Fixture file not found" Error
Run the fixture generation script first:
```bash
uv run python scripts/generate_test_fixtures.py your_excel_file.xlsx
```

### Fixtures Too Large
Adjust sampling percentages in the script or use the small fixtures for unit tests.

### Different Results Between Runs
Ensure you're using the same seed value (default: 42) for reproducibility.