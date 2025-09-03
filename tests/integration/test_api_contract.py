"""
API Contract Validation Tests

Tests to ensure API response schemas remain consistent and
match documented contracts.
"""
import pytest
from core.deterministic.concentration import ConcentrationAnalyzer
import pandas as pd


class TestAPIContract:
    """Test API response schema consistency."""
    
    def setup_method(self):
        self.analyzer = ConcentrationAnalyzer()
    
    def test_concentration_result_schema(self):
        """Test ConcentrationResult maintains consistent schema."""
        # Create test data
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D"],
            "revenue": [100, 80, 60, 40]  # Total: 280
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=[10, 20, 50])
        
        # Validate top-level result structure
        assert hasattr(result, 'data')
        assert hasattr(result, 'parameters')
        assert hasattr(result, 'computation_log')
        assert hasattr(result, 'formulas')
        
        # Validate data structure
        assert isinstance(result.data, dict)
        assert "TOTAL" in result.data
        assert "summary" in result.data
        
        # Validate parameters structure
        params = result.parameters
        required_param_fields = ["group_by", "value_column", "thresholds", "total_rows", "analysis_type"]
        for field in required_param_fields:
            assert field in params
        
        assert params["group_by"] == "entity"
        assert params["value_column"] == "revenue"
        assert params["thresholds"] == [10, 20, 50]
        assert params["analysis_type"] in ["single_period", "multi_period"]
        
        # Validate computation log structure
        assert isinstance(result.computation_log, list)
        assert len(result.computation_log) > 0
        
        for log_entry in result.computation_log:
            assert isinstance(log_entry, dict)
            assert "step" in log_entry
            assert "status" in log_entry
        
        # Validate formulas structure
        assert isinstance(result.formulas, dict)
        expected_formulas = ["aggregation", "sorting", "cumulative_percentage", "top_10", "top_20", "top_50"]
        for formula in expected_formulas:
            assert formula in result.formulas
            assert isinstance(result.formulas[formula], str)
    
    def test_single_period_data_schema(self):
        """Test single-period analysis data schema."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [100, 50, 25]  # Total: 175
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        total_data = result.data["TOTAL"]
        
        # Validate required fields
        required_fields = ["period", "total_entities", "total_value", "concentration", "head_sample"]
        for field in required_fields:
            assert field in total_data
        
        # Validate field types
        assert isinstance(total_data["period"], str)
        assert isinstance(total_data["total_entities"], int)
        assert isinstance(total_data["total_value"], float)
        assert isinstance(total_data["concentration"], dict)
        assert isinstance(total_data["head_sample"], list)
        
        # Validate concentration schema
        concentration = total_data["concentration"]
        for threshold in ["top_10", "top_20", "top_50"]:
            assert threshold in concentration
            threshold_data = concentration[threshold]
            
            # Required threshold fields
            required_threshold_fields = ["count", "value", "percentage", "entities"]
            for field in required_threshold_fields:
                assert field in threshold_data
            
            # Validate threshold field types
            assert isinstance(threshold_data["count"], int)
            assert isinstance(threshold_data["value"], float)
            assert isinstance(threshold_data["percentage"], float)
            assert isinstance(threshold_data["entities"], list)
            
            # Validate ranges
            assert threshold_data["count"] > 0
            assert threshold_data["value"] > 0
            assert 0 <= threshold_data["percentage"] <= 100
            assert len(threshold_data["entities"]) <= 10  # Capped for display
        
        # Validate head sample schema
        head_sample = total_data["head_sample"]
        assert len(head_sample) <= 20  # Limited for display
        
        for item in head_sample:
            assert isinstance(item, dict)
            # Should contain at least the group_by and value columns
            assert "entity" in item
            assert "revenue" in item
            # Should also contain cumulative data
            assert "cumsum" in item
            assert "cumulative_pct" in item
    
    def test_multi_period_data_schema(self):
        """Test multi-period analysis data schema."""
        df = pd.DataFrame({
            "entity": ["A", "A", "B", "B"],
            "period": ["Q1", "Q2", "Q1", "Q2"],
            "revenue": [100, 120, 50, 60]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", period_key_column="period")
        
        # Should have multiple periods + TOTAL
        periods = [k for k in result.data.keys() if k != "summary"]
        assert len(periods) >= 3  # Q1, Q2, TOTAL
        assert "TOTAL" in periods
        
        # Each period should have same schema as single period
        for period_key in periods:
            period_data = result.data[period_key]
            
            # Basic schema validation
            required_fields = ["period", "total_entities", "total_value", "concentration", "head_sample"]
            for field in required_fields:
                assert field in period_data
            
            # Type validation
            assert isinstance(period_data["period"], str)
            assert isinstance(period_data["total_entities"], int)
            assert isinstance(period_data["total_value"], float)
            assert isinstance(period_data["concentration"], dict)
            assert isinstance(period_data["head_sample"], list)
        
        # Validate summary schema for multi-period
        summary = result.data["summary"]
        assert summary["analysis_type"] == "multi_period"
        assert "periods" in summary
        assert isinstance(summary["periods"], list)
        
        # Each period summary should have required fields
        for period_summary in summary["periods"]:
            assert "period" in period_summary
            assert "entities" in period_summary
            assert "value" in period_summary
    
    def test_error_response_schema(self):
        """Test error response schema consistency."""
        # Create data that will cause an error (all zeros)
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [0, 0, 0]  # Total: 0
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should have error in TOTAL
        assert "TOTAL" in result.data
        total_data = result.data["TOTAL"]
        assert "error" in total_data
        
        # Error should be standardized string
        assert isinstance(total_data["error"], str)
        assert "Total value is non-positive; cannot compute concentration" == total_data["error"]
        
        # Should still have computation log
        assert len(result.computation_log) > 0
        
        # Should have parameters even on error
        assert "group_by" in result.parameters
        assert "value_column" in result.parameters
    
    def test_json_serializable_contract(self):
        """Test that all results are JSON serializable."""
        import json
        
        # Test normal case
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [100.5, 80.7, 60.3]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should be fully JSON serializable
        try:
            json.dumps(result.data)
            json.dumps(result.parameters)
            json.dumps(result.computation_log)
            json.dumps(result.formulas)
        except TypeError as e:
            pytest.fail(f"API response not JSON serializable: {e}")
    
    def test_api_response_format_compatibility(self):
        """Test compatibility with expected API response format."""
        df = pd.DataFrame({
            "customer": ["Customer_A", "Customer_B", "Customer_C"],
            "sales": [1000, 500, 250]
        })
        
        result = self.analyzer.analyze(df, "customer", "sales")
        
        # Simulate API response transformation
        total_data = result.data["TOTAL"]
        concentration = total_data.get("concentration", {})
        
        # Simulate API response format
        api_format = {
            "period": total_data["period"],
            "total": total_data["total_value"],
            "top_10": {
                "count": concentration["top_10"]["count"],
                "value": concentration["top_10"]["value"],
                "pct_of_total": concentration["top_10"]["percentage"],
            } if "top_10" in concentration else None,
            "top_20": {
                "count": concentration["top_20"]["count"],
                "value": concentration["top_20"]["value"],
                "pct_of_total": concentration["top_20"]["percentage"],
            } if "top_20" in concentration else None,
            "top_50": {
                "count": concentration["top_50"]["count"],
                "value": concentration["top_50"]["value"],
                "pct_of_total": concentration["top_50"]["percentage"],
            } if "top_50" in concentration else None,
            "head": total_data["head_sample"][:10],  # Capped to 10 for API
        }
        
        # Validate API format structure
        assert "period" in api_format
        assert "total" in api_format
        assert "top_10" in api_format
        assert "top_20" in api_format
        assert "top_50" in api_format
        assert "head" in api_format
        
        # Validate head sample capping
        assert len(api_format["head"]) <= 10
        
        # Should be JSON serializable
        import json
        json.dumps(api_format)
    
    def test_threshold_customization_schema(self):
        """Test schema consistency with custom thresholds."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D", "E"],
            "value": [100, 80, 60, 40, 20]
        })
        
        custom_thresholds = [5, 15, 75]
        result = self.analyzer.analyze(df, "entity", "value", thresholds=custom_thresholds)
        
        # Parameters should reflect custom thresholds
        assert result.parameters["thresholds"] == custom_thresholds
        
        # Concentration should have custom threshold keys
        concentration = result.data["TOTAL"]["concentration"]
        expected_keys = ["top_5", "top_15", "top_75"]
        for key in expected_keys:
            assert key in concentration
            assert "count" in concentration[key]
            assert "value" in concentration[key]
            assert "percentage" in concentration[key]
            assert "entities" in concentration[key]
        
        # Should not have default thresholds
        default_keys = ["top_10", "top_20", "top_50"]
        for key in default_keys:
            assert key not in concentration
        
        # Formulas should reflect custom thresholds
        for threshold in custom_thresholds:
            assert f"top_{threshold}" in result.formulas
    
    def test_backwards_compatibility_all_total(self):
        """Test backwards compatibility for ALL vs TOTAL periods."""
        df = pd.DataFrame({
            "entity": ["A", "B"],
            "revenue": [100, 50]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # New format should use TOTAL
        assert "TOTAL" in result.data
        
        # Should not have ALL (deprecated)
        assert "ALL" not in result.data
        
        # Period field should be "TOTAL"
        assert result.data["TOTAL"]["period"] == "TOTAL"