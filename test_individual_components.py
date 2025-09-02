"""Test individual components to isolate the issue"""
import pandas as pd
from services.registry import DatasetRegistry
from services.normalization_service import NormalizationService
from core.deterministic.time import TimeDetector

# Create test data
data = {
    "date": ["2023-01-15", "2023-02-20"],
    "entity": ["Company_A", "Company_B"],
    "revenue": [1000.50, 500.00]
}
df = pd.DataFrame(data)

print("Testing individual components:")

# Test 1: Registry
print("\n1. Testing DatasetRegistry...")
try:
    registry = DatasetRegistry()
    dataset_id = registry.create_dataset("test.csv")
    print(f"✓ Registry created dataset: {dataset_id}")
except Exception as e:
    print(f"✗ Registry failed: {e}")

# Test 2: Normalization
print("\n2. Testing NormalizationService...")
try:
    norm_service = NormalizationService()
    norm_result = norm_service.normalize_and_persist(
        df, dataset_id, {"original_filename": "test.csv", "sheet": None}
    )
    print(f"✓ Normalization completed, schema has {len(norm_result.schema.get('columns', []))} columns")
except Exception as e:
    print(f"✗ Normalization failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Time Detection
print("\n3. Testing TimeDetector...")
try:
    time_detector = TimeDetector()
    time_result = time_detector.detect_time_dimensions(df)
    print(f"✓ Time detection completed: {time_result['period_grain']}")
except Exception as e:
    print(f"✗ Time detection failed: {e}")
    import traceback
    traceback.print_exc()

print("\nTest completed.")