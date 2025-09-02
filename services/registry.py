"""
Dataset Registry Service
Manages dataset lifecycle, schema, and lineage tracking.
"""
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from config.settings import settings


class DatasetRegistry:
    """Manages dataset lifecycle and metadata."""
    
    def __init__(self):
        self.storage_path = settings.datasets_path
        self.storage_path.mkdir(parents=True, exist_ok=True)
    
    def create_dataset(self, original_filename: str) -> str:
        """
        Create a new dataset with unique ID and folder structure.
        
        Args:
            original_filename: Original uploaded filename
        
        Returns:
            Dataset ID
        """
        dataset_id = f"ds_{uuid.uuid4().hex[:12]}"
        dataset_path = self.storage_path / dataset_id
        
        # Create dataset folder structure
        dataset_path.mkdir(parents=True, exist_ok=True)
        (dataset_path / "raw").mkdir(exist_ok=True)
        (dataset_path / "analyses").mkdir(exist_ok=True)
        (dataset_path / "llm").mkdir(exist_ok=True)
        
        # Initialize lineage
        lineage = {
            "dataset_id": dataset_id,
            "created_at": datetime.utcnow().isoformat(),
            "original_filename": original_filename,
            "steps": []
        }
        
        self._save_json(dataset_path / "lineage.json", lineage)
        
        return dataset_id
    
    def get_dataset_state(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get current state of a dataset.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Dataset state information
        """
        dataset_path = self.storage_path / dataset_id
        
        if not dataset_path.exists():
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Check what files exist
        has_raw = (dataset_path / "raw").exists() and any((dataset_path / "raw").iterdir())
        has_normalized = (dataset_path / "normalized.parquet").exists()
        has_schema = (dataset_path / "schema.json").exists()
        has_analyses = (dataset_path / "analyses").exists() and any((dataset_path / "analyses").iterdir())
        
        return {
            "dataset_id": dataset_id,
            "exists": True,
            "has_raw": has_raw,
            "has_normalized": has_normalized,
            "has_schema": has_schema,
            "has_analyses": has_analyses,
            "path": str(dataset_path)
        }
    
    def append_lineage_step(
        self,
        dataset_id: str,
        operation: str,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        llm_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Append a step to the dataset's lineage.
        
        Args:
            dataset_id: Dataset identifier
            operation: Operation name
            inputs: Input artifacts
            outputs: Output artifacts
            params: Operation parameters
            metrics: Computed metrics
            llm_info: LLM usage information
        
        Returns:
            Step ID
        """
        lineage_path = self.storage_path / dataset_id / "lineage.json"
        
        if not lineage_path.exists():
            raise ValueError(f"Lineage file not found for dataset {dataset_id}")
        
        lineage = self._load_json(lineage_path)
        
        step_id = f"st_{len(lineage['steps']) + 1:04d}"
        step = {
            "id": step_id,
            "timestamp": datetime.utcnow().isoformat(),
            "operation": operation,
            "inputs": inputs or [],
            "outputs": outputs or [],
            "params": params or {},
            "metrics": metrics or {}
        }
        
        if llm_info:
            step["llm"] = llm_info
        
        lineage["steps"].append(step)
        self._save_json(lineage_path, lineage)
        
        return step_id
    
    def save_schema(self, dataset_id: str, schema: Dict[str, Any]) -> None:
        """
        Save schema for a dataset.
        
        Args:
            dataset_id: Dataset identifier
            schema: Schema information
        """
        schema_path = self.storage_path / dataset_id / "schema.json"
        schema["dataset_id"] = dataset_id
        schema["created_at"] = datetime.utcnow().isoformat()
        self._save_json(schema_path, schema)
    
    def get_schema(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get schema for a dataset.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Schema if exists, None otherwise
        """
        schema_path = self.storage_path / dataset_id / "schema.json"
        
        if schema_path.exists():
            return self._load_json(schema_path)
        
        return None
    
    def get_lineage(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get lineage for a dataset.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Lineage if exists, None otherwise
        """
        lineage_path = self.storage_path / dataset_id / "lineage.json"
        
        if lineage_path.exists():
            return self._load_json(lineage_path)
        
        return None
    
    def record_llm_artifact(
        self,
        dataset_id: str,
        artifact_name: str,
        content: Dict[str, Any]
    ) -> str:
        """
        Record an LLM artifact (prompt/response).
        
        Args:
            dataset_id: Dataset identifier
            artifact_name: Name of the artifact
            content: Artifact content
        
        Returns:
            Path to saved artifact
        """
        llm_path = self.storage_path / dataset_id / "llm"
        llm_path.mkdir(exist_ok=True)
        
        artifact_path = llm_path / f"{artifact_name}.json"
        content["timestamp"] = datetime.utcnow().isoformat()
        self._save_json(artifact_path, content)
        
        return str(artifact_path)
    
    def _save_json(self, path: Path, data: Dict[str, Any]) -> None:
        """Save data as JSON."""
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def _load_json(self, path: Path) -> Dict[str, Any]:
        """Load JSON data."""
        with open(path, "r") as f:
            return json.load(f)