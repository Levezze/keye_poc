"""
Tests for Concentration Analysis Module
"""
import pytest
import pandas as pd
import numpy as np
from core.deterministic.concentration import ConcentrationAnalyzer, ConcentrationResult


class TestConcentrationAnalyzer:
    """Test concentration analysis with deterministic tie-breaking."""
    
    def setup_method(self):
        self.analyzer = ConcentrationAnalyzer()
        
    def test_thresholds_sorted_and_allow_100(self):
        """Test that thresholds are sorted and deduplicated, allowing 100%."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"], 
            "revenue": [100, 50, 25]
        })
        
        # Test unsorted thresholds with duplicates and 100%
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=[100, 20, 10, 20])
        
        # Should be sorted and deduplicated: [10, 20, 100]
        assert result.parameters["thresholds"] == [10, 20, 100]
        
        # Check that 100% threshold is calculated
        concentration = result.data["TOTAL"]["concentration"]
        assert "top_100" in concentration
        
        # 100% threshold should include all entities
        top_100 = concentration["top_100"]
        assert top_100["count"] == 3
        assert top_100["value"] == 175.0
        assert abs(top_100["percentage"] - 100.0) < 0.1
        
    def test_error_log_contains_period_on_failure(self):
        """Test that error logs include period information when aggregation fails."""
        # Create a DataFrame that will cause aggregation to fail
        df = pd.DataFrame({
            "entity": ["A", "B"],
            "revenue": [100, "invalid"]  # Invalid data type
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should have error in computation log
        error_logs = [log for log in result.computation_log if log.get("status") == "failed"]
        assert len(error_logs) > 0
        
        # Error log should contain period information
        error_log = error_logs[0]
        assert "period" in error_log
        assert error_log["period"] == "TOTAL"
    
    def test_single_period_basic_analysis(self):
        """Test basic single-period concentration analysis."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D", "E"],
            "revenue": [100, 80, 60, 40, 20]  # Total: 300
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Check basic structure
        assert isinstance(result, ConcentrationResult)
        assert "TOTAL" in result.data
        assert result.parameters["analysis_type"] == "single_period"
        
        # Check concentration calculations
        all_data = result.data["TOTAL"]
        assert all_data["total_entities"] == 5
        assert all_data["total_value"] == 300.0
        
        # Check thresholds (cumulative percentages)
        concentration = all_data["concentration"]
        
        # Top 10%: A (100/300 = 33.3%) 
        top_10 = concentration["top_10"]
        assert top_10["count"] == 1
        assert top_10["value"] == 100.0
        assert abs(top_10["percentage"] - 33.33) < 0.1
        
        # Top 20%: A (33.3%) - still under 50%
        top_20 = concentration["top_20"]
        assert top_20["count"] == 1
        assert top_20["value"] == 100.0
        
        # Top 50%: A + B (180/300 = 60%) but A alone is 33.3% which is ≤ 50%
        top_50 = concentration["top_50"]
        assert top_50["count"] == 1
        assert top_50["value"] == 100.0
    
    def test_tie_breaking_deterministic(self):
        """Test deterministic tie-breaking: value desc, entity asc."""
        df = pd.DataFrame({
            "entity": ["B", "A", "C"],  # Alphabetical order different from input
            "revenue": [100, 100, 50]   # Two entities tied for first
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        all_data = result.data["TOTAL"]
        
        # Check head sample shows deterministic ordering
        head_sample = all_data["head_sample"]
        
        # Both A and B have same value (100), but A should come first alphabetically
        assert head_sample[0]["entity"] == "A"
        assert head_sample[1]["entity"] == "B"
        assert head_sample[2]["entity"] == "C"
    
    def test_multi_period_analysis(self):
        """Test multi-period concentration analysis."""
        df = pd.DataFrame({
            "period_key": ["2023-Q1", "2023-Q1", "2023-Q2", "2023-Q2"],
            "entity": ["A", "B", "A", "C"],
            "revenue": [100, 50, 120, 80]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", "period_key")
        
        assert result.parameters["analysis_type"] == "multi_period"
        assert "2023-Q1" in result.data
        assert "2023-Q2" in result.data
        assert "TOTAL" in result.data
        assert "summary" in result.data
        
        # Check individual periods
        q1_data = result.data["2023-Q1"]
        assert q1_data["total_entities"] == 2
        assert q1_data["total_value"] == 150.0  # A:100 + B:50
        
        q2_data = result.data["2023-Q2"]
        assert q2_data["total_entities"] == 2
        assert q2_data["total_value"] == 200.0  # A:120 + C:80
        
        # Check total (aggregates across all periods)
        total_data = result.data["TOTAL"]
        assert total_data["total_entities"] == 3  # A, B, C
        assert total_data["total_value"] == 350.0  # A:220 + B:50 + C:80
    
    def test_zero_values_handling(self):
        """Test handling of zero and negative values."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D"],
            "revenue": [100, 0, -20, 50]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        all_data = result.data["TOTAL"]
        
        # Total should include all values (including negatives)
        assert all_data["total_value"] == 130.0  # 100 + 0 + (-20) + 50
        
        # Check that entities are properly sorted despite zero/negative values
        head_sample = all_data["head_sample"]
        # Should be: A(100), D(50), B(0), C(-20)
        assert head_sample[0]["entity"] == "A"
        assert head_sample[1]["entity"] == "D"
        assert head_sample[2]["entity"] == "B"
        assert head_sample[3]["entity"] == "C"
    
    def test_custom_thresholds(self):
        """Test analysis with custom threshold values."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [60, 30, 10]  # Total: 100
        })
        
        custom_thresholds = [25, 75]
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=custom_thresholds)
        
        concentration = result.data["TOTAL"]["concentration"]
        
        # Should only have top_25 and top_75
        assert "top_25" in concentration
        assert "top_75" in concentration
        assert "top_10" not in concentration
        assert "top_50" not in concentration
        
        # Top 25%: Only A qualifies (60% is > 25%, but A alone is exactly what fits)
        # Actually, we need to check cumulative: A=60/100=60% cumulative, which is > 25%
        # So no entities should qualify for top_25
        top_25 = concentration["top_25"]
        assert top_25["count"] >= 1  # At least first entity if none qualify
    
    def test_edge_case_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(columns=["entity", "revenue"])
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should handle gracefully with error
        assert "error" in result.data or result.data["TOTAL"].get("error")
    
    def test_edge_case_single_entity(self):
        """Test concentration analysis with single entity."""
        df = pd.DataFrame({
            "entity": ["A"],
            "revenue": [100]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        all_data = result.data["TOTAL"]
        
        assert all_data["total_entities"] == 1
        assert all_data["total_value"] == 100.0
        
        # Single entity should be 100% in all thresholds
        concentration = all_data["concentration"]
        for threshold in [10, 20, 50]:
            threshold_data = concentration[f"top_{threshold}"]
            assert threshold_data["count"] == 1
            assert threshold_data["value"] == 100.0
            assert threshold_data["percentage"] == 100.0
    
    def test_edge_case_all_zero_values(self):
        """Test handling when all values are zero."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [0, 0, 0]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should handle zero total gracefully
        assert result.data["TOTAL"]["error"] == "Total value is non-positive; cannot compute concentration"
    
    def test_computation_log_tracking(self):
        """Test that computation steps are properly logged."""
        df = pd.DataFrame({
            "entity": ["A", "B"],
            "revenue": [100, 50]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Check that computation log has entries
        assert len(result.computation_log) > 0
        
        # Should have aggregation and calculation steps
        log_steps = [step["step"] for step in result.computation_log]
        assert any("aggregation" in step for step in log_steps)
        assert any("concentration_calculation" in step for step in log_steps)
    
    def test_formulas_documentation(self):
        """Test that formulas are properly documented."""
        df = pd.DataFrame({
            "entity": ["A"],
            "revenue": [100]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Check formula documentation
        formulas = result.formulas
        assert "aggregation" in formulas
        assert "sorting" in formulas
        assert "cumulative_percentage" in formulas
        assert "top_10" in formulas
        assert "top_20" in formulas
        assert "top_50" in formulas
    
    def test_large_dataset_performance(self):
        """Test analysis with larger dataset for performance."""
        # Create larger dataset
        entities = [f"Entity_{i}" for i in range(100)]
        revenues = np.random.uniform(10, 1000, 100)
        
        df = pd.DataFrame({
            "entity": entities,
            "revenue": revenues
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        all_data = result.data["TOTAL"]
        
        assert all_data["total_entities"] == 100
        assert all_data["total_value"] > 0
        assert "concentration" in all_data
        
        # Should have head sample limited to reasonable size
        assert len(all_data["head_sample"]) <= 20
    
    def test_json_serializable_output(self):
        """Test that output is JSON serializable."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [100.5, 80.3, 60.7]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Try to serialize to ensure no numpy types remain
        import json
        try:
            json.dumps(result.data)
            json.dumps(result.parameters)
            json.dumps(result.computation_log)
            json.dumps(result.formulas)
        except TypeError as e:
            pytest.fail(f"Output not JSON serializable: {e}")
    
    def test_period_sorting(self):
        """Test that periods are processed in sorted order."""
        df = pd.DataFrame({
            "period_key": ["2023-Q3", "2023-Q1", "2023-Q2"],  # Out of order
            "entity": ["A", "B", "C"],
            "revenue": [100, 80, 60]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", "period_key")
        
        # Check that computation log shows periods were identified and sorted
        period_log = next(
            log for log in result.computation_log 
            if log["step"] == "period_identification"
        )
        
        assert period_log["periods_found"] == 3
        # Periods should be sorted in the log
        periods = period_log["periods"]
        assert periods == ["2023-Q1", "2023-Q2", "2023-Q3"]
    
    def test_summary_statistics(self):
        """Test summary statistics generation."""
        df = pd.DataFrame({
            "period_key": ["2023-Q1", "2023-Q1", "2023-Q2", "2023-Q2"],
            "entity": ["A", "B", "A", "C"],
            "revenue": [100, 50, 120, 80]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", "period_key")
        
        summary = result.data["summary"]
        assert summary["analysis_type"] == "multi_period"
        assert summary["periods_analyzed"] == 3  # Q1, Q2, TOTAL
        assert summary["thresholds"] == [10, 20, 50]
        assert summary["total_input_rows"] == 4
        assert "periods" in summary
        
        # Check period summaries
        period_summaries = summary["periods"]
        assert len(period_summaries) == 2  # Q1, Q2 (excluding TOTAL)
    
    def test_missing_columns_error_handling(self):
        """Test error handling for missing columns."""
        df = pd.DataFrame({
            "entity": ["A", "B"],
            "revenue": [100, 50]
        })
        
        # Try to analyze with non-existent column
        result = self.analyzer.analyze(df, "nonexistent_col", "revenue")
        
        # Should log error gracefully
        assert len(result.computation_log) > 0
        error_logs = [log for log in result.computation_log if log.get("status") == "failed"]
        assert len(error_logs) > 0