"""Debug concentration analysis endpoint"""
import pandas as pd
from fastapi.testclient import TestClient
from api.main import app

def create_test_csv() -> bytes:
    """Create a test CSV file."""
    data = {
        "date": ["2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05", "2023-05-12"],
        "entity": ["Company_A", "Company_B", "Company_C", "Company_A", "Company_B"],
        "revenue": [1000.50, 500.00, 750.25, 1200.75, 600.30],
        "region": ["North", "South", "East", "North", "South"]
    }
    df = pd.DataFrame(data)
    return df.to_csv(index=False).encode('utf-8')

# Test the complete workflow
client = TestClient(app)
csv_content = create_test_csv()

# Upload
print("Uploading file...")
upload_response = client.post(
    "/api/v1/upload",
    files={"file": ("test_data.csv", csv_content, "text/csv")}
)

print(f"Upload status: {upload_response.status_code}")
if upload_response.status_code != 200:
    print(f"Upload error: {upload_response.text}")
    exit()

dataset_id = upload_response.json()["dataset_id"]
print(f"Dataset ID: {dataset_id}")

# Get schema
print("\nGetting schema...")
schema_response = client.get(f"/api/v1/schema/{dataset_id}")
print(f"Schema status: {schema_response.status_code}")
if schema_response.status_code != 200:
    print(f"Schema error: {schema_response.text}")
else:
    schema_data = schema_response.json()
    print(f"Period grain: {schema_data['period_grain']}")

# Run concentration
print("\nRunning concentration analysis...")
concentration_request = {
    "group_by": "entity",
    "value": "revenue",
    "thresholds": [50, 75]
}

analysis_response = client.post(
    f"/api/v1/analyze/{dataset_id}/concentration",
    json=concentration_request
)

print(f"Analysis status: {analysis_response.status_code}")
if analysis_response.status_code != 200:
    print(f"Analysis error: {analysis_response.text}")
else:
    print("Analysis successful!")
    result = analysis_response.json()
    print(f"Period grain: {result['period_grain']}")
    print(f"Number of periods: {len(result['by_period'])}")