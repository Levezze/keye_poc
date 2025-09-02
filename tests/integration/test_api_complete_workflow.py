"""
Integration test for complete API workflow
Tests the full pipeline: upload → schema → analyze → download
"""

import pytest
import pandas as pd
import tempfile
import json
from pathlib import Path
from fastapi.testclient import TestClient
from api.main import app
from config.settings import settings


class TestCompleteAPIWorkflow:
    """Test complete API workflow integration."""

    def setup_method(self):
        """Set up test client and test data."""
        self.client = TestClient(app)
        self.headers = {}

        # Add API key if required
        if settings.api_key:
            self.headers["X-API-Key"] = settings.api_key

    def create_test_csv(self) -> bytes:
        """Create a test CSV file with time dimension and entities."""
        data = {
            "date": [
                "2023-01-15",
                "2023-02-20",
                "2023-03-10",
                "2023-01-15",
                "2023-02-20",
            ],
            "entity": ["Company_A", "Company_A", "Company_A", "Company_B", "Company_B"],
            "revenue": [1000.50, 1200.75, 980.25, 500.00, 750.50],
            "region": ["North", "North", "North", "South", "South"],
        }
        df = pd.DataFrame(data)
        return df.to_csv(index=False).encode("utf-8")

    def create_test_excel(self) -> bytes:
        """Create a test Excel file with time dimensions."""
        data = {
            "year": [2023, 2023, 2023, 2024, 2024],
            "quarter": [1, 2, 3, 1, 2],
            "customer": [
                "ACME Corp",
                "TechStart Inc",
                "Global Ltd",
                "ACME Corp",
                "NewCo",
            ],
            "sales": [10000, 5000, 8000, 12000, 6000],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            df.to_excel(tmp.name, index=False)
            with open(tmp.name, "rb") as f:
                content = f.read()

        Path(tmp.name).unlink()  # Clean up
        return content

    def test_complete_csv_workflow(self):
        """Test complete workflow with CSV file."""
        # Step 1: Upload CSV file
        csv_content = self.create_test_csv()

        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test_data.csv", csv_content, "text/csv")},
            headers=self.headers,
        )

        assert upload_response.status_code == 200
        upload_data = upload_response.json()

        assert "dataset_id" in upload_data
        assert upload_data["status"] == "completed"
        assert upload_data["rows_processed"] == 5
        assert upload_data["columns_processed"] == 4

        dataset_id = upload_data["dataset_id"]

        # Step 2: Get schema information
        schema_response = self.client.get(
            f"/api/v1/schema/{dataset_id}", headers=self.headers
        )

        assert schema_response.status_code == 200
        schema_data = schema_response.json()

        assert schema_data["dataset_id"] == dataset_id
        assert schema_data["period_grain"] == "date"  # Should detect date column
        assert "date" in schema_data["time_candidates"]
        assert len(schema_data["columns"]) == 5  # Added period_key column

        # Verify column roles
        column_names = [col["name"] for col in schema_data["columns"]]
        assert "date" in column_names
        assert "entity" in column_names
        assert "revenue" in column_names
        assert "region" in column_names

        # Step 3: Run concentration analysis
        concentration_request = {
            "group_by": "entity",
            "value": "revenue",
            "thresholds": [50, 75],  # Custom thresholds
        }

        analysis_response = self.client.post(
            f"/api/v1/analyze/{dataset_id}/concentration",
            json=concentration_request,
            headers=self.headers,
        )

        assert analysis_response.status_code == 200
        analysis_data = analysis_response.json()

        assert analysis_data["dataset_id"] == dataset_id
        assert analysis_data["period_grain"] == "date"
        assert analysis_data["thresholds"] == [50, 75]
        assert "by_period" in analysis_data
        assert "totals" in analysis_data
        assert "export_links" in analysis_data

        # Verify export links
        export_links = analysis_data["export_links"]
        assert "csv" in export_links
        assert "xlsx" in export_links
        assert dataset_id in export_links["csv"]

        # Step 4: Download CSV export
        csv_download_response = self.client.get(
            export_links["csv"], headers=self.headers
        )

        assert csv_download_response.status_code == 200
        assert (
            csv_download_response.headers["content-type"] == "text/csv; charset=utf-8"
        )

        # Verify CSV content is not empty
        csv_content = csv_download_response.content.decode("utf-8")
        assert len(csv_content) > 0
        assert "entity" in csv_content  # Should contain the group_by column

        # Step 5: Download Excel export
        xlsx_download_response = self.client.get(
            export_links["xlsx"], headers=self.headers
        )

        assert xlsx_download_response.status_code == 200
        assert "spreadsheetml" in xlsx_download_response.headers["content-type"]

        # Step 6: Get insights
        insights_response = self.client.get(
            f"/api/v1/insights/{dataset_id}", headers=self.headers
        )

        assert insights_response.status_code == 200
        insights_data = insights_response.json()

        assert insights_data["dataset_id"] == dataset_id
        assert "key_findings" in insights_data
        assert "recommendations" in insights_data
        assert len(insights_data["key_findings"]) > 0

    def test_complete_excel_workflow(self):
        """Test complete workflow with Excel file."""
        # Step 1: Upload Excel file
        excel_content = self.create_test_excel()

        upload_response = self.client.post(
            "/api/v1/upload",
            files={
                "file": (
                    "test_data.xlsx",
                    excel_content,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
            headers=self.headers,
        )

        assert upload_response.status_code == 200
        upload_data = upload_response.json()

        assert upload_data["status"] == "completed"
        dataset_id = upload_data["dataset_id"]

        # Step 2: Get schema - should detect year+quarter time dimension
        schema_response = self.client.get(
            f"/api/v1/schema/{dataset_id}", headers=self.headers
        )

        assert schema_response.status_code == 200
        schema_data = schema_response.json()

        assert (
            schema_data["period_grain"] == "year_quarter"
        )  # Should detect year+quarter
        assert "year" in schema_data["time_candidates"]
        assert "quarter" in schema_data["time_candidates"]

        # Step 3: Run concentration analysis
        concentration_request = {"group_by": "customer", "value": "sales"}

        analysis_response = self.client.post(
            f"/api/v1/analyze/{dataset_id}/concentration",
            json=concentration_request,
            headers=self.headers,
        )

        assert analysis_response.status_code == 200
        analysis_data = analysis_response.json()

        assert analysis_data["period_grain"] == "year_quarter"

        # Should have multiple periods in by_period
        by_period = analysis_data["by_period"]
        assert len(by_period) > 0  # Should have quarterly data

        # Verify period key format
        if by_period:
            period_keys = [period["period"] for period in by_period]
            # Should contain year-quarter format like "2023-Q1", "2023-Q2", etc.
            assert any("Q" in period for period in period_keys)

    def test_error_handling(self):
        """Test error handling in workflow."""
        # Test non-existent dataset (using proper format but non-existent ID)
        response = self.client.get(
            "/api/v1/schema/ds_000000000000", headers=self.headers
        )

        # Correct behavior: non-existent datasets should return 404
        assert response.status_code == 404

        # Test invalid file upload
        response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.txt", b"invalid content", "text/plain")},
            headers=self.headers,
        )

        assert response.status_code == 400  # Should reject non-CSV/Excel files

    def test_no_time_dimension_workflow(self):
        """Test workflow with data that has no time dimension."""
        # Create data without time columns
        data = {
            "product": ["Widget", "Gadget", "Tool"],
            "category": ["A", "B", "A"],
            "price": [10.99, 25.50, 15.75],
        }
        df = pd.DataFrame(data)
        csv_content = df.to_csv(index=False).encode("utf-8")

        # Upload
        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("no_time.csv", csv_content, "text/csv")},
            headers=self.headers,
        )

        assert upload_response.status_code == 200
        dataset_id = upload_response.json()["dataset_id"]

        # Check schema - should have no time dimension
        schema_response = self.client.get(
            f"/api/v1/schema/{dataset_id}", headers=self.headers
        )

        assert schema_response.status_code == 200
        schema_data = schema_response.json()

        assert schema_data["period_grain"] == "none"
        assert "No temporal dimension detected" in " ".join(
            schema_data.get("warnings", [])
        )

        # Run concentration analysis - should work with single period
        concentration_request = {"group_by": "category", "value": "price"}

        analysis_response = self.client.post(
            f"/api/v1/analyze/{dataset_id}/concentration",
            json=concentration_request,
            headers=self.headers,
        )

        assert analysis_response.status_code == 200
        analysis_data = analysis_response.json()

        assert analysis_data["period_grain"] == "none"
        assert len(analysis_data["by_period"]) == 0  # No periods
        assert "totals" in analysis_data  # But should have totals

    def test_missing_columns_error(self):
        """Test error handling for missing columns in concentration analysis."""
        # Upload valid data
        csv_content = self.create_test_csv()

        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test_data.csv", csv_content, "text/csv")},
            headers=self.headers,
        )

        assert upload_response.status_code == 200
        dataset_id = upload_response.json()["dataset_id"]

        # Try analysis with non-existent column
        concentration_request = {"group_by": "nonexistent_column", "value": "revenue"}

        analysis_response = self.client.post(
            f"/api/v1/analyze/{dataset_id}/concentration",
            json=concentration_request,
            headers=self.headers,
        )

        assert analysis_response.status_code == 400
        assert "not found in dataset" in analysis_response.json()["detail"]

    @pytest.mark.skipif(not settings.api_key, reason="API key not configured")
    def test_api_key_validation(self):
        """Test API key validation when configured."""
        # Test request without API key
        response = self.client.get("/api/v1/schema/any_dataset")

        assert response.status_code == 401

        # Test request with wrong API key
        response = self.client.get(
            "/api/v1/schema/any_dataset", headers={"X-API-Key": "wrong_key"}
        )

        assert response.status_code == 401
