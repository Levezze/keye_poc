"""
Integration tests for normalization service with storage persistence.
"""
import pytest
import pandas as pd
import tempfile
from pathlib import Path

from services.normalization_service import NormalizationService
from services.registry import DatasetRegistry
from config.settings import settings


class TestNormalizationServiceIntegration:
    """Test normalization service with real storage."""
    
    @pytest.fixture
    def temp_storage(self):
        """Create temporary storage directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Temporarily override settings
            original_path = settings.datasets_path
            settings.datasets_path = Path(temp_dir) / "datasets"
            yield temp_dir
            settings.datasets_path = original_path
    
    @pytest.fixture
    def normalization_service(self, temp_storage):
        """Create normalization service with temp storage."""
        return NormalizationService()
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame({
            "  Revenue (USD)  ": ["$1,234.56", "$2,500.00", "($500.00)", "$1.5k"],
            "Margin %": ["15.5%", "12.0%", "8.2%", "22.1%"],
            "Transaction Date": ["2024-01-15", "2024-02-20", "2024-03-10", "2024-04-05"],
            "Active": ["Yes", "No", "Y", "N"],
            "Notes": ["Good", "Bad", "OK", "Excellent"]
        })
    
    def test_full_normalization_workflow(self, normalization_service, sample_dataframe):
        """Test complete normalization workflow with persistence."""
        # Create dataset
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_data.csv")
        
        # Normalize and persist
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=sample_dataframe,
            original_filename="test_data.csv"
        )
        
        # Verify result structure
        assert "dataset_id" in result
        assert "normalization_result" in result
        assert "files_created" in result
        assert "lineage_step_id" in result
        assert "statistics" in result
        assert "warnings" in result
        assert "checksum" in result
        
        # Verify files were created
        dataset_state = registry.get_dataset_state(dataset_id)
        assert dataset_state["has_normalized"] == True
        assert dataset_state["has_schema"] == True
        
        # Verify we can load the data back
        loaded_df = normalization_service.get_normalized_data(dataset_id)
        assert loaded_df is not None
        assert len(loaded_df) == len(sample_dataframe)
        
        # Check that data was actually normalized
        assert "revenue_usd" in loaded_df.columns
        assert "margin" in loaded_df.columns
        
        # Verify numeric transformations worked
        revenue_col = loaded_df["revenue_usd"]
        assert abs(revenue_col.iloc[0] - 1234.56) < 0.01  # $1,234.56
        assert abs(revenue_col.iloc[1] - 2500.00) < 0.01  # $2,500.00
        assert abs(revenue_col.iloc[2] - (-500.00)) < 0.01  # ($500.00)
        assert abs(revenue_col.iloc[3] - 1500.0) < 0.01   # $1.5k
        
        # Verify percent normalization
        margin_col = loaded_df["margin"]
        assert abs(margin_col.iloc[0] - 0.155) < 0.001   # 15.5%
        assert abs(margin_col.iloc[1] - 0.120) < 0.001   # 12.0%
    
    def test_schema_information_accuracy(self, normalization_service, sample_dataframe):
        """Test that schema information is accurate and complete."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_schema.csv")
        
        # Normalize and persist
        normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=sample_dataframe,
            original_filename="test_schema.csv"
        )
        
        # Get schema info
        schema = normalization_service.get_schema_info(dataset_id)
        
        assert schema is not None
        assert schema["dataset_id"] == dataset_id
        assert "columns" in schema
        assert "metadata" in schema
        assert "warnings" in schema
        assert "transformations_summary" in schema
        
        # Verify column information
        columns_by_name = {col["name"]: col for col in schema["columns"]}
        
        # Check revenue column
        revenue_col = columns_by_name["revenue_usd"]
        assert revenue_col["role"] == "numeric"
        assert revenue_col["original_name"] == "  Revenue (USD)  "
        assert revenue_col["coercions"]["currency_removed"] > 0
        assert revenue_col["coercions"]["scaling_applied"] > 0
        
        # Check margin column
        margin_col = columns_by_name["margin"]
        assert margin_col["representation"] == "percent"
        assert margin_col["coercions"]["percent_normalized"] > 0
        
        # Check date column
        date_col = columns_by_name["transaction_date"]
        assert date_col["role"] == "datetime"
        assert date_col["coercions"]["datetime_parsed"] > 0
        
        # Check metadata
        assert schema["metadata"]["row_count"] == len(sample_dataframe)
        # Note: column count includes the added period_key column
        assert schema["metadata"]["column_count"] == len(sample_dataframe.columns) + 1
        assert schema["metadata"]["has_time_dimension"] == True
    
    def test_normalization_summary(self, normalization_service, sample_dataframe):
        """Test normalization summary generation."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_summary.csv")
        
        # Normalize and persist
        normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=sample_dataframe,
            original_filename="test_summary.csv"
        )
        
        # Get summary
        summary = normalization_service.get_normalization_summary(dataset_id)
        
        assert summary is not None
        assert summary["dataset_id"] == dataset_id
        assert "normalized_at" in summary
        assert "step_id" in summary
        assert "columns_info" in summary
        assert "metadata" in summary
        assert "warnings" in summary
        assert "transformation_summary" in summary
        assert "lineage_metrics" in summary
        assert "files_generated" in summary
        
        # Verify lineage metrics
        metrics = summary["lineage_metrics"]
        assert "transformations_applied" in metrics
        assert "warnings_generated" in metrics
        assert "parquet_checksum" in metrics
        
        # Verify files were generated
        files = summary["files_generated"]
        assert "normalized.parquet" in files
        assert "schema.json" in files
    
    def test_validation_success(self, normalization_service, sample_dataframe):
        """Test successful validation of normalized dataset."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_validation.csv")
        
        # Normalize and persist
        normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=sample_dataframe,
            original_filename="test_validation.csv"
        )
        
        # Validate
        validation = normalization_service.validate_normalization(dataset_id)
        
        assert validation["dataset_id"] == dataset_id
        assert validation["is_valid"] == True
        assert len(validation["errors"]) == 0
        # May have warnings for data quality issues, but should be valid
    
    def test_validation_missing_files(self, normalization_service):
        """Test validation when required files are missing."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_missing.csv")
        
        # Don't normalize - just create empty dataset
        
        # Validate should fail
        validation = normalization_service.validate_normalization(dataset_id)
        
        assert validation["dataset_id"] == dataset_id
        assert validation["is_valid"] == False
        assert len(validation["errors"]) > 0
        assert any("normalized.parquet" in error for error in validation["errors"])
        assert any("schema.json" in error for error in validation["errors"])
    
    def test_real_sample_data_integration(self, normalization_service):
        """Test integration with actual sample data files."""
        # Load real sample data
        sample_path = Path("tests/fixtures/sample_data/sample_edge_cases.csv")
        if not sample_path.exists():
            pytest.skip("Sample data file not found")
        
        df = pd.read_csv(sample_path)
        
        # Create and process dataset
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("sample_edge_cases.csv")
        
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=df,
            original_filename="sample_edge_cases.csv"
        )
        
        # Verify processing worked
        assert result["dataset_id"] == dataset_id
        assert len(result["warnings"]) >= 0  # May have warnings for negative revenues
        
        # Verify data is readable
        loaded_df = normalization_service.get_normalized_data(dataset_id)
        assert loaded_df is not None
        assert len(loaded_df) == len(df)
        
        # Verify schema was generated
        schema = normalization_service.get_schema_info(dataset_id)
        assert schema is not None
        
        # Should detect some numeric columns
        numeric_columns = [col for col in schema["columns"] if col["role"] == "numeric"]
        assert len(numeric_columns) > 0
        
        # Validation should pass
        validation = normalization_service.validate_normalization(dataset_id)
        assert validation["is_valid"] == True
    
    def test_error_handling_invalid_data(self, normalization_service):
        """Test error handling with invalid data."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_invalid.csv")
        
        # Create DataFrame with problematic data
        bad_df = pd.DataFrame({
            "bad_column": [None, None, None]  # All null column
        })
        
        # Should still work but may generate warnings
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=bad_df,
            original_filename="test_invalid.csv"
        )
        
        assert result["dataset_id"] == dataset_id
        # Should still create files even with problematic data
        validation = normalization_service.validate_normalization(dataset_id)
        assert validation["is_valid"] == True  # Files exist
        # May have data quality warnings
    
    def test_lineage_tracking_completeness(self, normalization_service, sample_dataframe):
        """Test that lineage tracking captures all necessary information."""
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("test_lineage.csv")
        
        # Normalize and persist
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=sample_dataframe,
            original_filename="test_lineage.csv"
        )
        
        # Get lineage
        lineage = registry.get_lineage(dataset_id)
        
        assert lineage is not None
        assert "steps" in lineage
        assert len(lineage["steps"]) >= 1  # At least the normalization step
        
        # Find normalization step
        normalize_step = None
        for step in lineage["steps"]:
            if step["operation"] == "normalize":
                normalize_step = step
                break
        
        assert normalize_step is not None
        assert normalize_step["id"] == result["lineage_step_id"]
        
        # Check step has all required information
        assert "timestamp" in normalize_step
        assert "inputs" in normalize_step
        assert "outputs" in normalize_step
        assert "params" in normalize_step
        assert "metrics" in normalize_step
        
        # Verify inputs and outputs
        assert "raw/test_lineage.csv" in normalize_step["inputs"]
        assert "normalized.parquet" in normalize_step["outputs"]
        assert "schema.json" in normalize_step["outputs"]
        
        # Verify metrics include checksum
        assert "parquet_checksum" in normalize_step["metrics"]
    
    def test_time_detection_integration(self, normalization_service):
        """Test that time detection is properly integrated into the normalization pipeline."""
        # Create DataFrame with time dimensions
        time_df = pd.DataFrame({
            "year": [2023, 2023, 2024, 2024],
            "month": [1, 6, 3, 9],
            "revenue": [1000, 1500, 1200, 1800],
            "product": ["A", "B", "A", "B"]
        })
        
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("time_test.csv")
        
        # Normalize and persist
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=time_df,
            original_filename="time_test.csv"
        )
        
        # Verify period_key column was added to normalized data
        loaded_df = normalization_service.get_normalized_data(dataset_id)
        assert "period_key" in loaded_df.columns
        
        # Verify period keys are properly formatted (YYYY-M## format)
        period_keys = loaded_df["period_key"].tolist()
        expected_keys = ["2023-M01", "2023-M06", "2024-M03", "2024-M09"]
        assert sorted(period_keys) == sorted(expected_keys)
        
        # Verify schema contains time-related fields
        schema = normalization_service.get_schema_info(dataset_id)
        assert "period_grain" in schema
        assert "period_grain_candidates" in schema  
        assert "time_candidates" in schema
        
        # Verify correct period grain detected
        assert schema["period_grain"] == "year_month"
        assert "year_month" in schema["period_grain_candidates"]
        assert "year" in schema["time_candidates"]
        assert "month" in schema["time_candidates"]
    
    def test_time_detection_no_time_columns(self, normalization_service):
        """Test graceful handling when no time columns are detected."""
        # Create DataFrame without time dimensions
        no_time_df = pd.DataFrame({
            "product": ["A", "B", "C"],
            "revenue": [100, 200, 150],
            "category": ["X", "Y", "Z"]
        })
        
        registry = normalization_service.registry
        dataset_id = registry.create_dataset("no_time_test.csv")
        
        # Normalize and persist
        result = normalization_service.normalize_and_persist(
            dataset_id=dataset_id,
            df=no_time_df,
            original_filename="no_time_test.csv"
        )
        
        # Verify period_key column defaults to "ALL"
        loaded_df = normalization_service.get_normalized_data(dataset_id)
        assert "period_key" in loaded_df.columns
        assert all(loaded_df["period_key"] == "ALL")
        
        # Verify schema shows no time detection
        schema = normalization_service.get_schema_info(dataset_id)
        assert schema["period_grain"] == "none"
        assert schema["time_candidates"] == []
        assert "No temporal dimension detected" in schema["warnings"]


if __name__ == "__main__":
    pytest.main([__file__])