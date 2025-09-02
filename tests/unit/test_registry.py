"""
Tests for DatasetRegistry service.
"""
import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import Dict, Any

from services.registry import DatasetRegistry


class TestDatasetRegistry:
    """Test cases for DatasetRegistry."""
    
    @pytest.fixture
    def registry(self, mock_datasets_path: Path) -> DatasetRegistry:
        """Create a registry instance with mocked path."""
        return DatasetRegistry()
    
    def test_create_dataset(self, registry: DatasetRegistry, mock_datasets_path: Path):
        """Test dataset creation with proper folder structure."""
        dataset_id = registry.create_dataset("test_file.xlsx")
        
        # Verify dataset ID format
        assert dataset_id.startswith("ds_")
        assert len(dataset_id) == 15  # "ds_" + 12 hex chars
        
        # Verify folder structure
        dataset_path = mock_datasets_path / dataset_id
        assert dataset_path.exists()
        assert (dataset_path / "raw").exists()
        assert (dataset_path / "analyses").exists()
        assert (dataset_path / "llm").exists()
        
        # Verify lineage file creation
        lineage_file = dataset_path / "lineage.json"
        assert lineage_file.exists()
        
        with open(lineage_file, "r") as f:
            lineage = json.load(f)
        
        assert lineage["dataset_id"] == dataset_id
        assert lineage["original_filename"] == "test_file.xlsx"
        assert "created_at" in lineage
        assert lineage["steps"] == []
        
        # Verify timestamp format
        created_at = datetime.fromisoformat(lineage["created_at"])
        assert isinstance(created_at, datetime)
    
    def test_get_dataset_state_existing(self, registry: DatasetRegistry, mock_datasets_path: Path):
        """Test getting state of an existing dataset."""
        dataset_id = registry.create_dataset("test.xlsx")
        dataset_path = mock_datasets_path / dataset_id
        
        # Create some test files
        (dataset_path / "raw" / "test.xlsx").touch()
        (dataset_path / "normalized.parquet").touch()
        (dataset_path / "schema.json").touch()
        (dataset_path / "analyses" / "analysis1.json").touch()
        
        state = registry.get_dataset_state(dataset_id)
        
        assert state["dataset_id"] == dataset_id
        assert state["exists"] is True
        assert state["has_raw"] is True
        assert state["has_normalized"] is True
        assert state["has_schema"] is True
        assert state["has_analyses"] is True
        assert state["path"] == str(dataset_path)
    
    def test_get_dataset_state_nonexistent(self, registry: DatasetRegistry):
        """Test getting state of non-existent dataset."""
        with pytest.raises(ValueError, match="Dataset nonexistent not found"):
            registry.get_dataset_state("nonexistent")
    
    def test_append_lineage_step_basic(self, registry: DatasetRegistry, sample_lineage_step: Dict[str, Any]):
        """Test appending a basic lineage step."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        step_id = registry.append_lineage_step(
            dataset_id,
            sample_lineage_step["operation"],
            inputs=sample_lineage_step["inputs"],
            outputs=sample_lineage_step["outputs"],
            params=sample_lineage_step["params"],
            metrics=sample_lineage_step["metrics"]
        )
        
        assert step_id == "st_0001"
        
        lineage = registry.get_lineage(dataset_id)
        assert len(lineage["steps"]) == 1
        
        step = lineage["steps"][0]
        assert step["id"] == step_id
        assert step["operation"] == sample_lineage_step["operation"]
        assert step["inputs"] == sample_lineage_step["inputs"]
        assert step["outputs"] == sample_lineage_step["outputs"]
        assert step["params"] == sample_lineage_step["params"]
        assert step["metrics"] == sample_lineage_step["metrics"]
        assert "timestamp" in step
        assert "llm" not in step  # LLM info not provided
    
    def test_append_lineage_step_with_llm(self, registry: DatasetRegistry, sample_lineage_step: Dict[str, Any]):
        """Test appending a lineage step with LLM info."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        step_id = registry.append_lineage_step(
            dataset_id,
            sample_lineage_step["operation"],
            inputs=sample_lineage_step["inputs"],
            outputs=sample_lineage_step["outputs"],
            params=sample_lineage_step["params"],
            metrics=sample_lineage_step["metrics"],
            llm_info=sample_lineage_step["llm_info"]
        )
        
        lineage = registry.get_lineage(dataset_id)
        step = lineage["steps"][0]
        
        assert "llm" in step
        assert step["llm"] == sample_lineage_step["llm_info"]
    
    def test_append_lineage_step_multiple(self, registry: DatasetRegistry):
        """Test appending multiple lineage steps."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        # Add first step
        step_id_1 = registry.append_lineage_step(
            dataset_id,
            "upload",
            outputs=["raw/test.xlsx"]
        )
        
        # Add second step
        step_id_2 = registry.append_lineage_step(
            dataset_id,
            "normalize",
            inputs=["raw/test.xlsx"],
            outputs=["normalized.parquet"]
        )
        
        assert step_id_1 == "st_0001"
        assert step_id_2 == "st_0002"
        
        lineage = registry.get_lineage(dataset_id)
        assert len(lineage["steps"]) == 2
        assert lineage["steps"][0]["id"] == step_id_1
        assert lineage["steps"][1]["id"] == step_id_2
    
    def test_append_lineage_step_nonexistent_dataset(self, registry: DatasetRegistry):
        """Test appending to non-existent dataset."""
        with pytest.raises(ValueError, match="Lineage file not found for dataset nonexistent"):
            registry.append_lineage_step("nonexistent", "test_op")
    
    def test_save_and_get_schema(self, registry: DatasetRegistry, sample_schema: Dict[str, Any]):
        """Test saving and retrieving schema."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        registry.save_schema(dataset_id, sample_schema)
        
        retrieved_schema = registry.get_schema(dataset_id)
        
        assert retrieved_schema is not None
        assert retrieved_schema["dataset_id"] == dataset_id
        assert "created_at" in retrieved_schema
        assert retrieved_schema["version"] == sample_schema["version"]
        assert retrieved_schema["columns"] == sample_schema["columns"]
        assert retrieved_schema["primary_key"] == sample_schema["primary_key"]
        
        # Verify timestamp format
        created_at = datetime.fromisoformat(retrieved_schema["created_at"])
        assert isinstance(created_at, datetime)
    
    def test_get_schema_nonexistent(self, registry: DatasetRegistry):
        """Test getting schema for dataset without schema."""
        dataset_id = registry.create_dataset("test.xlsx")
        schema = registry.get_schema(dataset_id)
        assert schema is None
    
    def test_get_lineage(self, registry: DatasetRegistry):
        """Test getting lineage."""
        dataset_id = registry.create_dataset("test.xlsx")
        lineage = registry.get_lineage(dataset_id)
        
        assert lineage is not None
        assert lineage["dataset_id"] == dataset_id
        assert lineage["original_filename"] == "test.xlsx"
        assert lineage["steps"] == []
        assert "created_at" in lineage
    
    def test_get_lineage_nonexistent(self, registry: DatasetRegistry):
        """Test getting lineage for non-existent dataset."""
        lineage = registry.get_lineage("nonexistent")
        assert lineage is None
    
    def test_record_llm_artifact(self, registry: DatasetRegistry):
        """Test recording LLM artifacts."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        artifact_content = {
            "prompt": "Analyze this data",
            "response": "The data shows...",
            "model": "gpt-4",
            "tokens": 150
        }
        
        artifact_path = registry.record_llm_artifact(
            dataset_id,
            "schema_analysis",
            artifact_content
        )
        
        # Verify the path
        expected_path = registry.storage_path / dataset_id / "llm" / "schema_analysis.json"
        assert artifact_path == str(expected_path)
        assert expected_path.exists()
        
        # Verify content
        with open(expected_path, "r") as f:
            saved_content = json.load(f)
        
        assert saved_content["prompt"] == artifact_content["prompt"]
        assert saved_content["response"] == artifact_content["response"]
        assert saved_content["model"] == artifact_content["model"]
        assert saved_content["tokens"] == artifact_content["tokens"]
        assert "timestamp" in saved_content
        
        # Verify timestamp format
        timestamp = datetime.fromisoformat(saved_content["timestamp"])
        assert isinstance(timestamp, datetime)
    
    def test_append_lineage_step_minimal_params(self, registry: DatasetRegistry):
        """Test appending lineage step with minimal parameters."""
        dataset_id = registry.create_dataset("test.xlsx")
        
        step_id = registry.append_lineage_step(dataset_id, "test_operation")
        
        lineage = registry.get_lineage(dataset_id)
        step = lineage["steps"][0]
        
        assert step["id"] == "st_0001"
        assert step["operation"] == "test_operation"
        assert step["inputs"] == []
        assert step["outputs"] == []
        assert step["params"] == {}
        assert step["metrics"] == {}
        assert "llm" not in step
        assert "timestamp" in step