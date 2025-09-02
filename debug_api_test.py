"""Debug API upload issue"""
import pandas as pd
from fastapi.testclient import TestClient
from api.main import app

def create_test_csv() -> bytes:
    """Create a test CSV file."""
    data = {
        "date": ["2023-01-15", "2023-02-20"],
        "entity": ["Company_A", "Company_B"],
        "revenue": [1000.50, 500.00]
    }
    df = pd.DataFrame(data)
    return df.to_csv(index=False).encode('utf-8')

# Test the upload
client = TestClient(app)
csv_content = create_test_csv()

upload_response = client.post(
    "/api/v1/upload",
    files={"file": ("test_data.csv", csv_content, "text/csv")}
)

print(f"Status code: {upload_response.status_code}")
if upload_response.status_code == 200:
    print(f"Response: {upload_response.json()}")
else:
    print(f"Error response: {upload_response.json()}")
    # Try to get more detailed error info
    import traceback
    print("\nFull response text:")
    print(upload_response.text)