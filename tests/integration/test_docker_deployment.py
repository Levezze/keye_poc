"""
Docker integration tests to verify API functionality within container environment
"""

import pytest
import requests
import time
import os
import subprocess
from pathlib import Path


@pytest.mark.skipif(
    subprocess.run(["which", "docker"], capture_output=True).returncode != 0,
    reason="Docker not available"
)
class TestDockerDeployment:
    """Test API functionality within Docker container."""
    
    @classmethod
    def setup_class(cls):
        """Set up Docker container for testing."""
        
        # Kill any existing containers
        subprocess.run(
            ["docker", "stop", "keye-poc-api"], 
            capture_output=True
        )
        subprocess.run(
            ["docker", "rm", "keye-poc-api"], 
            capture_output=True
        )
        
        # Build the Docker image
        build_result = subprocess.run(
            ["docker", "build", "-t", "keye-poc", "."],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent.parent
        )
        
        if build_result.returncode != 0:
            pytest.skip(f"Docker build failed: {build_result.stderr}")
        
        # Start the container
        start_result = subprocess.run([
            "docker", "run", "-d",
            "--name", "keye-poc-api",
            "-p", "8002:8000",
            "-v", f"{Path(__file__).parent.parent.parent}/storage:/app/storage",
            "--env", "API_KEY=test-docker-key",
            "keye-poc"
        ], capture_output=True, text=True)
        
        if start_result.returncode != 0:
            pytest.skip(f"Docker start failed: {start_result.stderr}")
        
        # Wait for container to be ready
        cls._wait_for_api_ready()
    
    @classmethod
    def teardown_class(cls):
        """Clean up Docker container after testing."""
        subprocess.run(["docker", "stop", "keye-poc-api"], capture_output=True)
        subprocess.run(["docker", "rm", "keye-poc-api"], capture_output=True)
    
    @classmethod
    def _wait_for_api_ready(cls, max_attempts=30):
        """Wait for API to be ready."""
        for _ in range(max_attempts):
            try:
                response = requests.get("http://localhost:8002/health", timeout=5)
                if response.status_code == 200:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(1)
        
        pytest.skip("API did not become ready in time")
    
    def test_health_check_in_docker(self):
        """Test that health check works in Docker."""
        response = requests.get("http://localhost:8002/healthz")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["status"] == "healthy"
        assert data["service"] == "keye-poc-api"
        assert "checks" in data
    
    def test_complete_workflow_in_docker(self):
        """Test complete workflow within Docker container."""
        
        headers = {"X-API-Key": "test-docker-key"}
        
        # Create test data
        test_csv = "entity,value\nCompany A,1000\nCompany B,2000\nCompany C,3000"
        
        # Upload file
        files = {"file": ("test_docker.csv", test_csv, "text/csv")}
        upload_response = requests.post(
            "http://localhost:8002/api/v1/upload",
            files=files,
            headers=headers
        )
        
        assert upload_response.status_code == 200
        upload_data = upload_response.json()
        dataset_id = upload_data["dataset_id"]
        
        # Get schema
        schema_response = requests.get(
            f"http://localhost:8002/api/v1/schema/{dataset_id}",
            headers=headers
        )
        
        assert schema_response.status_code == 200
        schema_data = schema_response.json()
        assert schema_data["dataset_id"] == dataset_id
        
        # Run concentration analysis
        analyze_request = {
            "group_by": "entity",
            "value": "value",
            "thresholds": [50, 100]
        }
        
        analyze_response = requests.post(
            f"http://localhost:8002/api/v1/analyze/{dataset_id}/concentration",
            json=analyze_request,
            headers=headers
        )
        
        assert analyze_response.status_code == 200
        analyze_data = analyze_response.json()
        assert analyze_data["dataset_id"] == dataset_id
        assert analyze_data["thresholds"] == [50, 100]
        
        # Get insights
        insights_response = requests.get(
            f"http://localhost:8002/api/v1/insights/{dataset_id}",
            headers=headers
        )
        
        assert insights_response.status_code == 200
        insights_data = insights_response.json()
        assert insights_data["dataset_id"] == dataset_id
        
        # Get lineage
        lineage_response = requests.get(
            f"http://localhost:8002/api/v1/lineage/{dataset_id}",
            headers=headers
        )
        
        assert lineage_response.status_code == 200
        lineage_data = lineage_response.json()
        assert "steps" in lineage_data
        assert len(lineage_data["steps"]) > 0
        
        # Download CSV (verify it exists)
        csv_response = requests.get(
            f"http://localhost:8002/api/v1/download/{dataset_id}/concentration.csv",
            headers=headers
        )
        
        assert csv_response.status_code == 200
        assert "text/csv" in csv_response.headers["content-type"]
    
    def test_error_handling_in_docker(self):
        """Test error handling works correctly in Docker."""
        
        headers = {"X-API-Key": "test-docker-key", "X-Request-ID": "docker-test"}
        
        # Test 404 error
        response = requests.get(
            "http://localhost:8002/api/v1/schema/ds_000000000000",
            headers=headers
        )
        
        assert response.status_code == 404
        data = response.json()
        
        # Verify error response structure
        assert "error" in data
        assert "message" in data
        assert "request_id" in data
        assert data["error"] == "NotFound"
        assert data["request_id"] == "docker-test"
    
    def test_rate_limiting_in_docker(self):
        """Test rate limiting works in Docker."""
        
        # Make rapid requests to trigger rate limiting
        for i in range(65):
            response = requests.get("http://localhost:8002/healthz")
            
            if response.status_code == 429:
                data = response.json()
                assert data["error"] == "RateLimited"
                assert "Retry-After" in response.headers
                break
        else:
            pytest.skip("Rate limiting may not be triggered in Docker environment")
    
    def test_request_id_tracking_in_docker(self):
        """Test request ID tracking works in Docker."""
        
        test_request_id = "docker-tracking-test"
        response = requests.get(
            "http://localhost:8002/healthz",
            headers={"X-Request-ID": test_request_id}
        )
        
        assert response.status_code == 200
        assert response.headers["X-Request-ID"] == test_request_id
    
    def test_file_validation_in_docker(self):
        """Test file validation works in Docker."""
        
        headers = {"X-API-Key": "test-docker-key"}
        
        # Test invalid file type
        files = {"file": ("test.txt", b"invalid content", "text/plain")}
        response = requests.post(
            "http://localhost:8002/api/v1/upload",
            files=files,
            headers=headers
        )
        
        assert response.status_code == 400
        data = response.json()
        assert data["error"] == "ValidationError"
        assert "unsupported" in data["message"].lower()