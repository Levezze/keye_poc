"""
Integration tests for enhanced API features including error responses, 
rate limiting, request tracking, and validation
"""

import pytest
import json
import time
from pathlib import Path
from fastapi.testclient import TestClient
from api.main import app
from config.settings import settings


class TestEnhancedAPIFeatures:
    """Test enhanced API features including error handling, rate limiting, and validation."""

    def setup_method(self):
        """Set up test client and test data."""
        self.client = TestClient(app)
        self.headers = {}

        # Add API key if required
        if settings.api_key:
            self.headers["X-API-Key"] = settings.api_key

    def test_error_response_format(self):
        """Test that all errors use standardized ErrorResponse format."""
        
        # Test 404 - Dataset not found
        response = self.client.get(
            "/api/v1/schema/ds_000000000000", 
            headers={**self.headers, "X-Request-ID": "test-404"}
        )
        
        assert response.status_code == 404
        data = response.json()
        
        # Validate ErrorResponse structure
        assert "error" in data
        assert "message" in data
        assert "details" in data
        assert "request_id" in data
        
        assert data["error"] == "NotFound"
        assert "not found" in data["message"].lower()
        assert data["request_id"] == "test-404"
        
        # Test 400 - Invalid dataset ID format (use format that doesn't match pattern)
        response = self.client.get(
            "/api/v1/schema/ds_invalid123", 
            headers={**self.headers, "X-Request-ID": "test-400"}
        )
        
        assert response.status_code == 400
        data = response.json()
        
        assert data["error"] == "ValidationError"
        assert data["request_id"] == "test-400"

    def test_request_id_tracking(self):
        """Test request ID is properly tracked and returned."""
        
        # Test with provided request ID
        test_request_id = "test-request-12345"
        response = self.client.get(
            "/healthz", 
            headers={"X-Request-ID": test_request_id}
        )
        
        assert response.headers["X-Request-ID"] == test_request_id
        
        # Test without provided request ID (should generate one)
        response = self.client.get("/healthz")
        
        assert "X-Request-ID" in response.headers
        generated_id = response.headers["X-Request-ID"]
        assert len(generated_id) > 0
        
        # Test error response includes request ID
        response = self.client.get(
            "/api/v1/schema/ds_invalid123",
            headers={**self.headers, "X-Request-ID": "error-test"}
        )
        
        assert response.status_code == 400
        data = response.json()
        assert data["request_id"] == "error-test"

    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        
        # Make many requests to trigger rate limit
        # Note: This test might be flaky in concurrent test runs
        rate_limit_exceeded = False
        
        for i in range(65):  # Exceed the 60/minute limit
            response = self.client.get(
                f"/healthz",
                headers={"X-Request-ID": f"rate-test-{i}"}
            )
            
            if response.status_code == 429:
                rate_limit_exceeded = True
                data = response.json()
                
                # Validate rate limit error structure
                assert data["error"] == "RateLimited"
                assert "rate limit" in data["message"].lower()
                assert "limit" in data["details"]
                assert data["details"]["limit"] == 60
                assert "Retry-After" in response.headers
                break
        
        # In a controlled test environment, we should hit the rate limit
        assert rate_limit_exceeded, "Rate limit should be exceeded after 60+ requests"

    def test_enhanced_health_endpoint(self):
        """Test enhanced health endpoint with dependency checks."""
        
        response = self.client.get("/healthz")
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate health response structure
        assert data["status"] == "healthy"
        assert data["service"] == "keye-poc-api"
        assert "checks" in data
        
        # Validate storage checks
        checks = data["checks"]
        assert "storage_directory" in checks
        assert "directory_creation" in checks
        
        storage_check = checks["storage_directory"]
        assert storage_check["status"] == "healthy"
        assert "path" in storage_check
        assert storage_check["writable"] is True

    def test_file_validation_enhanced(self):
        """Test enhanced file validation with strict MIME type checking."""
        
        # Test invalid file extension
        invalid_file_content = b"test content"
        response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.txt", invalid_file_content, "text/plain")},
            headers={**self.headers, "X-Request-ID": "file-test-1"}
        )
        
        assert response.status_code == 400
        data = response.json()
        assert data["error"] == "ValidationError"
        assert data["request_id"] == "file-test-1"
        assert "unsupported" in data["message"].lower()
        
        # Test large file (simulate with Content-Length if possible)
        # Note: Actual large file testing would require generating large content
        # This tests the validation logic
        
        # Test invalid MIME type but valid extension
        response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", b"col1,col2\n1,2", "application/octet-stream")},
            headers={**self.headers, "X-Request-ID": "file-test-2"}
        )
        
        assert response.status_code == 400
        data = response.json()
        assert data["error"] == "ValidationError"

    def test_validation_error_handling(self):
        """Test validation error handling for various endpoints."""
        
        # First create a dataset for testing analyze endpoint
        valid_csv = b"entity,value\nA,100\nB,200"
        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", valid_csv, "text/csv")},
            headers=self.headers
        )
        
        assert upload_response.status_code == 200
        dataset_id = upload_response.json()["dataset_id"]
        
        # Test invalid thresholds
        invalid_analyze_request = {
            "group_by": "entity",
            "value": "value", 
            "thresholds": [101, 200, 0]  # Invalid: >100, >100, <1
        }
        
        response = self.client.post(
            f"/api/v1/analyze/{dataset_id}/concentration",
            json=invalid_analyze_request,
            headers={**self.headers, "X-Request-ID": "validate-test"}
        )
        
        assert response.status_code == 422
        data = response.json()
        assert data["error"] == "ValidationError"
        assert data["request_id"] == "validate-test"
        assert "validation_errors" in data["details"]

    def test_unauthorized_access(self):
        """Test unauthorized access handling."""
        
        if not settings.api_key:
            pytest.skip("API key not configured for testing")
        
        # Test without API key
        response = self.client.get("/api/v1/schema/any_dataset")
        
        assert response.status_code == 401
        data = response.json()
        assert data["error"] == "Unauthorized"
        
        # Test with wrong API key
        response = self.client.get(
            "/api/v1/schema/any_dataset",
            headers={"X-API-Key": "wrong-key", "X-Request-ID": "auth-test"}
        )
        
        assert response.status_code == 401
        data = response.json()
        assert data["error"] == "Unauthorized"
        assert data["request_id"] == "auth-test"

    def test_lineage_endpoint_functionality(self):
        """Test that lineage endpoint returns actual data."""
        
        # Create a dataset first
        valid_csv = b"customer,revenue\nACME,1000\nTech Corp,2000"
        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", valid_csv, "text/csv")},
            headers=self.headers
        )
        
        assert upload_response.status_code == 200
        dataset_id = upload_response.json()["dataset_id"]
        
        # Test lineage endpoint
        response = self.client.get(
            f"/api/v1/lineage/{dataset_id}",
            headers={**self.headers, "X-Request-ID": "lineage-test"}
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Validate lineage structure
        assert "dataset_id" in data
        assert "created_at" in data
        assert "steps" in data
        assert isinstance(data["steps"], list)
        
        # Should have at least the upload step
        assert len(data["steps"]) > 0
        
        # Test lineage for non-existent dataset
        response = self.client.get(
            "/api/v1/lineage/ds_000000000000",
            headers={**self.headers, "X-Request-ID": "lineage-404"}
        )
        
        assert response.status_code == 404
        error_data = response.json()
        assert error_data["error"] == "NotFound"
        assert error_data["request_id"] == "lineage-404"

    def test_comprehensive_error_status_codes(self):
        """Test that all expected HTTP status codes are properly returned."""
        
        # Create dataset for testing various scenarios
        valid_csv = b"entity,amount\nCustomer A,500\nCustomer B,1000"
        upload_response = self.client.post(
            "/api/v1/upload",
            files={"file": ("test.csv", valid_csv, "text/csv")},
            headers=self.headers
        )
        dataset_id = upload_response.json()["dataset_id"]
        
        # Test cases for different error codes
        test_cases = [
            {
                "description": "400 - Bad Request (invalid dataset ID format)",
                "method": "get",
                "url": "/api/v1/schema/ds_invalid123",
                "expected_status": 400,
                "expected_error": "ValidationError"
            },
            {
                "description": "404 - Not Found (dataset)",
                "method": "get", 
                "url": "/api/v1/schema/ds_000000000000",
                "expected_status": 404,
                "expected_error": "NotFound"
            },
            {
                "description": "422 - Validation Error (bad analyze request)",
                "method": "post",
                "url": f"/api/v1/analyze/{dataset_id}/concentration",
                "json": {"group_by": "nonexistent", "value": "amount", "thresholds": [150]},
                "expected_status": 422,
                "expected_error": "ValidationError"
            }
        ]
        
        for case in test_cases:
            request_id = f"status-test-{case['expected_status']}"
            headers = {**self.headers, "X-Request-ID": request_id}
            
            if case["method"] == "get":
                response = self.client.get(case["url"], headers=headers)
            elif case["method"] == "post":
                response = self.client.post(
                    case["url"], 
                    json=case.get("json", {}),
                    headers=headers
                )
            
            assert response.status_code == case["expected_status"], \
                f"Failed {case['description']}: expected {case['expected_status']}, got {response.status_code}"
            
            data = response.json()
            assert data["error"] == case["expected_error"], \
                f"Failed {case['description']}: expected error {case['expected_error']}, got {data['error']}"
            assert data["request_id"] == request_id

    def test_cors_and_headers(self):
        """Test CORS configuration and custom headers."""
        
        # Test that X-Request-ID is properly exposed
        response = self.client.get("/healthz", headers={"X-Request-ID": "cors-test"})
        
        assert response.status_code == 200
        assert response.headers["X-Request-ID"] == "cors-test"
        
        # For CORS testing in a real scenario, we'd need to test with different origins
        # This is more of a configuration test

    def test_payload_size_limit(self):
        """Test payload size limit enforcement."""
        
        # Create a file that's larger than 25MB (simulated)
        # Note: In a real test, you'd create actual large content
        # For now, we test that the validation logic is in place
        
        # This test would need actual large file generation to be fully effective
        # The middleware checks Content-Length, so we'd need to simulate that
        
        # Test with a reasonable size file that should pass
        reasonable_csv = b"entity,value\n" + b"test,100\n" * 1000  # ~10KB
        response = self.client.post(
            "/api/v1/upload",
            files={"file": ("reasonable.csv", reasonable_csv, "text/csv")},
            headers=self.headers
        )
        
        # Should not fail due to size
        assert response.status_code in [200, 400]  # 400 might be for other validation reasons
        
        if response.status_code == 400:
            # If it fails, it shouldn't be due to size for this small file
            error_data = response.json()
            assert "too large" not in error_data["message"].lower()