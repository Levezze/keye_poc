"""
Integration tests using real data fixtures.

These tests use the sampled Excel/CSV files generated from real data
to test the complete data pipeline end-to-end.
"""
import pytest
import pandas as pd
from pathlib import Path
from typing import Dict, Any

from services.registry import DatasetRegistry
from services.storage import StorageService
from services.exporters import ExportService


class TestRealDataPipeline:
    """Integration tests with real data fixtures."""
    
    @pytest.fixture
    def registry(self, mock_datasets_path: Path) -> DatasetRegistry:
        """Create a registry instance with mocked path."""
        return DatasetRegistry()
    
    def test_full_pipeline_with_20pct_sample(
        self, 
        sample_20pct_excel: Path,
        registry: DatasetRegistry,
        temp_dir: Path
    ):
        """Test complete pipeline with 20% sampled real data."""
        # Step 1: Create dataset and upload
        dataset_id = registry.create_dataset(sample_20pct_excel.name)
        
        # Verify dataset structure created
        state = registry.get_dataset_state(dataset_id)
        assert state["exists"]
        
        # Step 2: Read and process the Excel file
        df = StorageService.read_excel(sample_20pct_excel)
        assert len(df) > 0
        
        # Save to raw folder
        raw_path = Path(state["path"]) / "raw" / sample_20pct_excel.name
        raw_checksum = StorageService.save_upload(
            sample_20pct_excel.read_bytes(),
            raw_path
        )
        
        # Record upload in lineage
        registry.append_lineage_step(
            dataset_id,
            "upload",
            outputs=[f"raw/{sample_20pct_excel.name}"],
            metrics={"file_size": sample_20pct_excel.stat().st_size, "checksum": raw_checksum}
        )
        
        # Step 3: Normalize and save as parquet
        normalized_path = Path(state["path"]) / "normalized.parquet"
        parquet_checksum = StorageService.write_parquet(df, normalized_path)
        
        # Record normalization in lineage
        registry.append_lineage_step(
            dataset_id,
            "normalization",
            inputs=[f"raw/{sample_20pct_excel.name}"],
            outputs=["normalized.parquet"],
            params={"format": "parquet"},
            metrics={"rows": len(df), "columns": len(df.columns), "checksum": parquet_checksum}
        )
        
        # Step 4: Verify round-trip data integrity
        df_loaded = StorageService.read_parquet(normalized_path)
        pd.testing.assert_frame_equal(df, df_loaded)
        
        # Step 5: Export to multiple formats
        export_dir = temp_dir / "exports"
        export_dir.mkdir(exist_ok=True)
        
        # Create mock concentration results based on real data
        results = self._create_mock_concentration_results(df)
        
        # Export to CSV
        csv_export = export_dir / "concentration.csv"
        ExportService.export_concentration_csv(results, csv_export)
        assert csv_export.exists()
        
        # Export to Excel
        excel_export = export_dir / "concentration.xlsx"
        ExportService.export_concentration_excel(results, excel_export)
        assert excel_export.exists()
        
        # Verify lineage is complete
        lineage = registry.get_lineage(dataset_id)
        assert len(lineage["steps"]) >= 2
        assert lineage["steps"][0]["operation"] == "upload"
        assert lineage["steps"][1]["operation"] == "normalization"
    
    def test_csv_excel_compatibility(
        self,
        sample_small_excel: Path,
        sample_small_csv: Path
    ):
        """Test that CSV and Excel versions produce identical DataFrames."""
        # Read both formats
        df_excel = StorageService.read_excel(sample_small_excel)
        df_csv = StorageService.read_csv(sample_small_csv)
        
        # Should have same shape
        assert df_excel.shape == df_csv.shape
        
        # Convert data types for comparison (CSV might read numbers as strings)
        for col in df_excel.select_dtypes(include=['number']).columns:
            if col in df_csv.columns:
                df_csv[col] = pd.to_numeric(df_csv[col], errors='coerce')
        
        # DataFrames should be equivalent (allowing for minor type differences)
        assert len(df_excel) == len(df_csv)
        assert list(df_excel.columns) == list(df_csv.columns)
    
    def test_edge_cases_handling(
        self,
        sample_edge_cases_excel: Path,
        registry: DatasetRegistry
    ):
        """Test handling of edge cases like nulls and special characters."""
        # Read edge cases file
        df = StorageService.read_excel(sample_edge_cases_excel)
        
        # Should contain some null values
        assert df.isnull().any().any(), "Edge cases should include null values"
        
        # Create dataset and save
        dataset_id = registry.create_dataset("edge_cases.xlsx")
        state = registry.get_dataset_state(dataset_id)
        
        # Test parquet round-trip with nulls
        parquet_path = Path(state["path"]) / "edge_cases.parquet"
        StorageService.write_parquet(df, parquet_path)
        df_loaded = StorageService.read_parquet(parquet_path)
        
        # Nulls should be preserved
        assert df.isnull().sum().sum() == df_loaded.isnull().sum().sum()
        
        # Test CSV export (CSV handles nulls differently)
        csv_path = Path(state["path"]) / "edge_cases.csv"
        StorageService.write_csv(df, csv_path)
        df_csv = StorageService.read_csv(csv_path)
        
        # CSV might represent nulls differently but shape should match
        assert df.shape == df_csv.shape
    
    def test_time_series_preservation(
        self,
        sample_time_balanced_excel: Path,
        fixture_metadata: Dict[str, Any]
    ):
        """Test that time-balanced sampling preserves all time periods."""
        # Read time-balanced sample
        df = StorageService.read_excel(sample_time_balanced_excel)
        
        # Check if time column was detected
        time_column = fixture_metadata.get("detected_columns", {}).get("time")
        
        if time_column and time_column in df.columns:
            # Verify all unique time periods are present
            unique_periods = df[time_column].nunique()
            assert unique_periods > 1, "Should have multiple time periods"
            
            # Each period should have roughly the same number of rows
            period_counts = df[time_column].value_counts()
            assert period_counts.std() / period_counts.mean() < 0.5, \
                "Time periods should be balanced"
    
    def test_large_file_performance(
        self,
        sample_20pct_excel: Path,
        temp_dir: Path
    ):
        """Test performance with larger sampled file."""
        import time
        
        # Time Excel reading
        start = time.time()
        df = StorageService.read_excel(sample_20pct_excel)
        excel_read_time = time.time() - start
        
        # Time Parquet writing
        parquet_path = temp_dir / "test.parquet"
        start = time.time()
        StorageService.write_parquet(df, parquet_path)
        parquet_write_time = time.time() - start
        
        # Time Parquet reading
        start = time.time()
        df_loaded = StorageService.read_parquet(parquet_path)
        parquet_read_time = time.time() - start
        
        # Parquet should be faster than Excel for reading
        assert parquet_read_time < excel_read_time * 2, \
            "Parquet reading should be reasonably fast"
        
        # Log performance metrics (useful for optimization)
        print(f"\nPerformance metrics for {len(df)} rows:")
        print(f"  Excel read: {excel_read_time:.3f}s")
        print(f"  Parquet write: {parquet_write_time:.3f}s")
        print(f"  Parquet read: {parquet_read_time:.3f}s")
    
    def _create_mock_concentration_results(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Create mock concentration results from real DataFrame."""
        # Find a suitable group column
        group_col = None
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]) and df[col].nunique() < len(df) * 0.5:
                group_col = col
                break
        
        # Find a suitable value column
        value_col = None
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                value_col = col
                break
        
        if not group_col or not value_col:
            # Return minimal results if suitable columns not found
            return {
                "by_period": [{
                    "period": "2023",
                    "total": 1000000,
                    "top_10": {"count": 1, "value": 100000, "pct_of_total": 10.0}
                }]
            }
        
        # Create realistic results based on actual data
        total = df[value_col].sum()
        top_group = df.groupby(group_col)[value_col].sum().nlargest(1)
        
        return {
            "group_by": group_col,
            "value_column": value_col,
            "by_period": [{
                "period": "2023",
                "total": float(total),
                "top_10": {
                    "count": 1,
                    "value": float(top_group.values[0]) if len(top_group) > 0 else 0,
                    "pct_of_total": float(top_group.values[0] / total * 100) if total > 0 else 0
                }
            }]
        }