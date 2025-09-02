"""
Normalization Service
High-level service that orchestrates data normalization with storage persistence.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional

from core.deterministic.normalization import DataNormalizer
from services.storage import StorageService
from services.registry import DatasetRegistry


class NormalizationService:
    """Orchestrates data normalization with storage persistence."""
    
    def __init__(self):
        self.normalizer = DataNormalizer()
        self.storage = StorageService()
        self.registry = DatasetRegistry()
    
    def normalize_and_persist(
        self,
        dataset_id: str,
        df: pd.DataFrame,
        original_filename: str
    ) -> Dict[str, Any]:
        """
        Normalize DataFrame and persist all artifacts.
        
        Args:
            dataset_id: Dataset identifier
            df: Input DataFrame to normalize
            original_filename: Original filename for lineage
        
        Returns:
            Dict with normalization results and file paths
        """
        # Get dataset paths
        dataset_state = self.registry.get_dataset_state(dataset_id)
        dataset_path = Path(dataset_state["path"])
        
        # Perform normalization
        result = self.normalizer.normalize(df)
        
        # Update schema with dataset_id
        result.schema["dataset_id"] = dataset_id
        
        # Save normalized data as Parquet
        normalized_path = dataset_path / "normalized.parquet"
        checksum = self.storage.write_parquet(result.data, normalized_path)
        
        # Save schema as JSON
        self.registry.save_schema(dataset_id, result.schema)
        
        # Create comprehensive lineage step
        lineage_step = self.registry.append_lineage_step(
            dataset_id=dataset_id,
            operation="normalize",
            inputs=[f"raw/{original_filename}"],
            outputs=["normalized.parquet", "schema.json"],
            params={
                "original_columns": len(df.columns),
                "original_rows": len(df),
                "normalized_columns": len(result.data.columns),
                "normalized_rows": len(result.data)
            },
            metrics={
                "transformations_applied": result.statistics["total_transformations"],
                "warnings_generated": result.statistics["warnings_count"],
                "columns_modified": result.schema["transformations_summary"]["columns_modified"],
                "transformation_types": result.schema["transformations_summary"]["transformation_types"],
                "parquet_checksum": checksum
            }
        )
        
        return {
            "dataset_id": dataset_id,
            "normalization_result": result,
            "files_created": {
                "normalized_data": str(normalized_path),
                "schema": str(dataset_path / "schema.json"),
                "lineage": str(dataset_path / "lineage.json")
            },
            "lineage_step_id": lineage_step,
            "statistics": result.statistics,
            "warnings": result.warnings,
            "checksum": checksum
        }
    
    def get_normalized_data(self, dataset_id: str) -> Optional[pd.DataFrame]:
        """
        Load normalized data for a dataset.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Normalized DataFrame if exists, None otherwise
        """
        dataset_state = self.registry.get_dataset_state(dataset_id)
        
        if not dataset_state["has_normalized"]:
            return None
        
        dataset_path = Path(dataset_state["path"])
        normalized_path = dataset_path / "normalized.parquet"
        
        return self.storage.read_parquet(normalized_path)
    
    def get_schema_info(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get schema information for a dataset.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Schema information if exists, None otherwise
        """
        return self.registry.get_schema(dataset_id)
    
    def get_normalization_summary(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a summary of normalization steps and results.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Normalization summary if available
        """
        schema = self.registry.get_schema(dataset_id)
        lineage = self.registry.get_lineage(dataset_id)
        
        if not schema or not lineage:
            return None
        
        # Find normalization step in lineage
        normalize_step = None
        for step in lineage.get("steps", []):
            if step.get("operation") == "normalize":
                normalize_step = step
                break
        
        if not normalize_step:
            return None
        
        return {
            "dataset_id": dataset_id,
            "normalized_at": normalize_step["timestamp"],
            "step_id": normalize_step["id"],
            "columns_info": schema["columns"],
            "metadata": schema["metadata"],
            "warnings": schema["warnings"],
            "transformation_summary": schema["transformations_summary"],
            "lineage_metrics": normalize_step.get("metrics", {}),
            "files_generated": normalize_step.get("outputs", [])
        }
    
    def validate_normalization(self, dataset_id: str) -> Dict[str, Any]:
        """
        Validate that normalization completed successfully.
        
        Args:
            dataset_id: Dataset identifier
        
        Returns:
            Validation results
        """
        dataset_state = self.registry.get_dataset_state(dataset_id)
        schema = self.registry.get_schema(dataset_id)
        
        validation_results = {
            "dataset_id": dataset_id,
            "is_valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check that all required files exist
        if not dataset_state["has_normalized"]:
            validation_results["is_valid"] = False
            validation_results["errors"].append("Missing normalized.parquet file")
        
        if not dataset_state["has_schema"]:
            validation_results["is_valid"] = False
            validation_results["errors"].append("Missing schema.json file")
        
        # Validate schema structure if it exists
        if schema:
            required_schema_keys = ["dataset_id", "columns", "metadata", "warnings"]
            missing_keys = [key for key in required_schema_keys if key not in schema]
            
            if missing_keys:
                validation_results["is_valid"] = False
                validation_results["errors"].append(f"Schema missing required keys: {missing_keys}")
            
            # Check if columns have required information
            if "columns" in schema:
                for i, col in enumerate(schema["columns"]):
                    required_col_keys = ["name", "dtype", "role", "coercions"]
                    missing_col_keys = [key for key in required_col_keys if key not in col]
                    
                    if missing_col_keys:
                        validation_results["warnings"].append(
                            f"Column {i} ({col.get('name', 'unknown')}) missing keys: {missing_col_keys}"
                        )
            
            # Check for data quality warnings
            if schema.get("warnings"):
                validation_results["warnings"].extend([
                    f"Data quality warning: {warning}" for warning in schema["warnings"]
                ])
        
        # Try to load normalized data and verify it's readable
        if dataset_state["has_normalized"]:
            try:
                df = self.get_normalized_data(dataset_id)
                if df is None or df.empty:
                    validation_results["is_valid"] = False
                    validation_results["errors"].append("Normalized data is empty or unreadable")
                else:
                    validation_results["warnings"].append(f"Normalized data contains {len(df)} rows, {len(df.columns)} columns")
            except Exception as e:
                validation_results["is_valid"] = False
                validation_results["errors"].append(f"Error reading normalized data: {str(e)}")
        
        return validation_results