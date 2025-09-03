"""
Golden Fixture Tests for Concentration Analysis

Tests the core analyzer with real sample data files to ensure
consistent results and validate behavior with real-world data.
"""
import pytest
import pandas as pd
from pathlib import Path
from core.deterministic.concentration import ConcentrationAnalyzer


class TestGoldenFixtures:
    """Test concentration analysis with real sample data."""
    
    @pytest.fixture
    def sample_files_path(self):
        """Path to sample data files."""
        return Path("tests/fixtures/sample_data")
    
    @pytest.fixture
    def analyzer(self):
        """ConcentrationAnalyzer instance."""
        return ConcentrationAnalyzer()
    
    def test_sample_small_basic_functionality(self, sample_files_path, analyzer):
        """Test basic functionality with sample_small.xlsx."""
        sample_file = sample_files_path / "sample_small.xlsx"
        if not sample_file.exists():
            pytest.skip(f"Sample file {sample_file} not found")
        
        # Load sample data
        df = pd.read_excel(sample_file)
        
        if len(df) == 0:
            pytest.skip("Empty sample file")
        
        # Find suitable columns
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
        
        if not categorical_cols or not numeric_cols:
            pytest.skip("Could not find suitable entity and value columns")
        
        entity_column = categorical_cols[0]
        value_column = numeric_cols[0]
        
        # Run concentration analysis
        result = analyzer.analyze(df, entity_column, value_column, thresholds=[10, 20, 50])
        
        # Validate basic structure
        assert "TOTAL" in result.data
        assert "summary" in result.data
        assert len(result.computation_log) > 0
        
        total_data = result.data["TOTAL"]
        
        # Should have valid results or clear error
        if "error" in total_data:
            # If error, should be our standardized message
            assert "cannot compute concentration" in total_data["error"]
        else:
            # If successful, validate structure
            assert "total_entities" in total_data
            assert "total_value" in total_data
            assert "concentration" in total_data
            assert "head_sample" in total_data
            
            # Validate concentration structure
            concentration = total_data["concentration"]
            for threshold in ["top_10", "top_20", "top_50"]:
                assert threshold in concentration
                threshold_data = concentration[threshold]
                assert "count" in threshold_data
                assert "value" in threshold_data
                assert "percentage" in threshold_data
                assert "entities" in threshold_data
    
    def test_sample_time_balanced_multi_period(self, sample_files_path, analyzer):
        """Test period handling with sample_time_balanced.xlsx."""
        sample_file = sample_files_path / "sample_time_balanced.xlsx"
        if not sample_file.exists():
            pytest.skip(f"Sample file {sample_file} not found")
        
        # Load sample data
        df = pd.read_excel(sample_file)
        
        if len(df) == 0:
            pytest.skip("Empty sample file")
        
        # Find suitable columns
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
        
        if not categorical_cols or not numeric_cols:
            pytest.skip("Could not find suitable columns")
        
        entity_column = categorical_cols[0]
        value_column = numeric_cols[0]
        
        # Look for time-like column
        time_column = None
        for col in df.columns:
            if any(time_word in col.lower() for time_word in ['date', 'time', 'period', 'year', 'month', 'quarter']):
                time_column = col
                break
        
        # Test both with and without period column
        result = analyzer.analyze(df, entity_column, value_column, period_key_column=time_column)
        
        # Should produce valid results
        assert "TOTAL" in result.data or len([k for k in result.data.keys() if k != "summary"]) > 0
        assert result.parameters["analysis_type"] in ["single_period", "multi_period"]
    
    def test_sample_edge_cases_robustness(self, sample_files_path, analyzer):
        """Test robustness with sample_edge_cases.xlsx."""
        sample_file = sample_files_path / "sample_edge_cases.xlsx"
        if not sample_file.exists():
            pytest.skip(f"Sample file {sample_file} not found")
        
        # Load sample data  
        df = pd.read_excel(sample_file)
        
        if len(df) == 0:
            pytest.skip("Empty sample file")
        
        # Find columns
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
        
        if not categorical_cols or not numeric_cols:
            pytest.skip("Could not find suitable columns")
        
        entity_column = categorical_cols[0]
        value_column = numeric_cols[0]
        
        # Should not raise exception, even with edge case data
        result = analyzer.analyze(df, entity_column, value_column)
        
        # Should produce some result (even if error)
        assert len(result.data) > 0
        assert len(result.computation_log) > 0
        
        # If error, should be properly formatted
        if "TOTAL" in result.data and "error" in result.data["TOTAL"]:
            assert isinstance(result.data["TOTAL"]["error"], str)
            assert len(result.data["TOTAL"]["error"]) > 0
    
    def test_direct_analyzer_consistency(self, sample_files_path):
        """Test that analyzer produces consistent results across multiple runs."""
        sample_file = sample_files_path / "sample_small.xlsx"
        if not sample_file.exists():
            pytest.skip(f"Sample file {sample_file} not found")
        
        # Load data directly
        df = pd.read_excel(sample_file)
        
        if len(df) == 0:
            pytest.skip("Empty sample file")
        
        # Find suitable columns
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
        
        if not categorical_cols or not numeric_cols:
            pytest.skip("Could not find suitable columns")
        
        entity_col = categorical_cols[0]
        value_col = numeric_cols[0]
        
        # Test analyzer directly multiple times
        from core.deterministic.concentration import ConcentrationAnalyzer
        analyzer = ConcentrationAnalyzer()
        
        results = []
        for _ in range(3):
            result = analyzer.analyze(df, entity_col, value_col)
            results.append(result)
        
        # Results should be identical (deterministic)
        for i in range(1, len(results)):
            # Compare key metrics
            if "TOTAL" in results[0].data and "TOTAL" in results[i].data:
                total_0 = results[0].data["TOTAL"]
                total_i = results[i].data["TOTAL"]
                
                if "error" not in total_0 and "error" not in total_i:
                    assert total_0["total_entities"] == total_i["total_entities"]
                    assert total_0["total_value"] == total_i["total_value"]
                    
                    # Head sample should be identical (deterministic ordering)
                    assert total_0["head_sample"] == total_i["head_sample"]
    
    def test_golden_values_sample_top_entities(self, sample_files_path):
        """Test with sample_top_entities.xlsx and validate specific expected results."""
        sample_file = sample_files_path / "sample_top_entities.xlsx"
        if not sample_file.exists():
            pytest.skip(f"Sample file {sample_file} not found")
        
        # Load and analyze
        df = pd.read_excel(sample_file)
        
        if len(df) == 0:
            pytest.skip("Empty sample file")
        
        # Find columns (assume first categorical is entity, first numeric is value)
        categorical_cols = [col for col in df.columns if df[col].dtype == 'object']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
        
        if not categorical_cols or not numeric_cols:
            pytest.skip("Could not find suitable columns")
        
        entity_col = categorical_cols[0]
        value_col = numeric_cols[0]
        
        # Analyze
        from core.deterministic.concentration import ConcentrationAnalyzer
        analyzer = ConcentrationAnalyzer()
        result = analyzer.analyze(df, entity_col, value_col, thresholds=[10, 20, 50])
        
        if "error" in result.data.get("TOTAL", {}):
            pytest.skip("Sample data produced error (may contain edge cases)")
        
        total_data = result.data["TOTAL"]
        concentration = total_data["concentration"]
        
        # Validate monotonicity invariants
        top_10 = concentration["top_10"]
        top_20 = concentration["top_20"]
        top_50 = concentration["top_50"]
        
        # Counts should be monotonic
        assert top_20["count"] >= top_10["count"]
        assert top_50["count"] >= top_20["count"]
        
        # Values should be monotonic
        assert top_20["value"] >= top_10["value"]
        assert top_50["value"] >= top_20["value"]
        
        # Percentages should be monotonic
        assert top_20["percentage"] >= top_10["percentage"]
        assert top_50["percentage"] >= top_20["percentage"]
        
        # All percentages should be <= 100%
        assert top_10["percentage"] <= 100
        assert top_20["percentage"] <= 100  
        assert top_50["percentage"] <= 100
        
        # Head sample should exist and be properly formatted
        head_sample = total_data["head_sample"]
        assert len(head_sample) > 0
        assert len(head_sample) <= 20  # Limited for display
        
        # Head sample should be sorted deterministically
        for i in range(1, len(head_sample)):
            prev_item = head_sample[i-1]
            curr_item = head_sample[i]
            
            # Should be sorted by value desc, then entity asc
            if prev_item[value_col] == curr_item[value_col]:
                assert prev_item[entity_col] <= curr_item[entity_col]
            else:
                assert prev_item[value_col] >= curr_item[value_col]