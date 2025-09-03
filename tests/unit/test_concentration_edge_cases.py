"""
Enhanced Edge Case Tests for Concentration Analysis
"""
import pytest
import pandas as pd
import numpy as np
from core.deterministic.concentration import ConcentrationAnalyzer, ConcentrationResult


class TestConcentrationEdgeCases:
    """Test enhanced edge cases for concentration analysis."""
    
    def setup_method(self):
        self.analyzer = ConcentrationAnalyzer()
    
    def test_all_negative_totals(self):
        """Test error handling for all-negative totals."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [-100, -50, -25]  # Total: -175
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should return standardized error message
        assert result.data["TOTAL"]["error"] == "Total value is non-positive; cannot compute concentration"
        # Note: The error is caught and logged in computation_log
        assert len(result.computation_log) > 0
    
    def test_mixed_sum_to_zero(self):
        """Test handling when positive and negative values sum exactly to zero."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D"],
            "revenue": [100, 50, -75, -75]  # Total: 0
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should return standardized error message
        assert result.data["TOTAL"]["error"] == "Total value is non-positive; cannot compute concentration"
    
    def test_very_small_positive_total(self):
        """Test handling of very small positive totals."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [0.001, 0.0005, 0.0003]  # Total: 0.0018
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        # Should work normally with small positive totals
        assert "TOTAL" in result.data
        assert "error" not in result.data["TOTAL"]
        assert result.data["TOTAL"]["total_value"] == pytest.approx(0.0018)
    
    def test_monotonicity_invariants(self):
        """Test that concentration thresholds maintain monotonicity invariants."""
        # Create dataset where we can predict the results
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D", "E"],
            "revenue": [100, 80, 60, 40, 20]  # Total: 300
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=[10, 20, 50])
        concentration = result.data["TOTAL"]["concentration"]
        
        # Monotonicity invariants: higher thresholds should include >= entities and >= value
        top_10 = concentration["top_10"]
        top_20 = concentration["top_20"] 
        top_50 = concentration["top_50"]
        
        # Count monotonicity: top_20 >= top_10, top_50 >= top_20
        assert top_20["count"] >= top_10["count"]
        assert top_50["count"] >= top_20["count"]
        
        # Value monotonicity: top_20 >= top_10, top_50 >= top_20
        assert top_20["value"] >= top_10["value"]
        assert top_50["value"] >= top_20["value"]
        
        # Percentage monotonicity should also hold
        assert top_20["percentage"] >= top_10["percentage"]
        assert top_50["percentage"] >= top_20["percentage"]
    
    def test_extreme_ties_deterministic(self):
        """Test deterministic tie-breaking with many identical values."""
        # Create 50 entities with identical values
        entities = [f"Entity_{i:02d}" for i in range(50)]
        revenues = [100] * 50  # All identical values
        
        df = pd.DataFrame({
            "entity": entities,
            "revenue": revenues
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        head_sample = result.data["TOTAL"]["head_sample"]
        
        # Should be deterministically sorted by entity name (ascending)
        sorted_entities = sorted(entities)
        
        # Check that head sample follows deterministic ordering
        for i in range(min(20, len(head_sample))):  # head_sample is limited to 20
            assert head_sample[i]["entity"] == sorted_entities[i]
        
        # Run same analysis multiple times to ensure consistency
        result2 = self.analyzer.analyze(df, "entity", "revenue")
        head_sample2 = result2.data["TOTAL"]["head_sample"]
        
        # Results should be identical across runs
        assert head_sample == head_sample2
    
    def test_property_based_random_validation(self):
        """Property-based test with random data to validate invariants."""
        # Use fixed seed for reproducibility
        np.random.seed(42)
        
        for _ in range(10):  # Run multiple random scenarios
            # Generate random dataset
            n_entities = np.random.randint(5, 100)
            entities = [f"E_{i}" for i in range(n_entities)]
            revenues = np.random.exponential(100, n_entities)  # Exponential distribution common in concentration
            
            df = pd.DataFrame({
                "entity": entities,
                "revenue": revenues
            })
            
            result = self.analyzer.analyze(df, "entity", "revenue")
            
            if "error" in result.data["TOTAL"]:
                continue  # Skip if error (shouldn't happen with exponential dist)
            
            concentration = result.data["TOTAL"]["concentration"]
            
            # Property: all counts should be positive
            for threshold in ["top_10", "top_20", "top_50"]:
                assert concentration[threshold]["count"] > 0
                assert concentration[threshold]["value"] > 0
                assert concentration[threshold]["percentage"] > 0
            
            # Property: percentages should never exceed 100%
            for threshold in ["top_10", "top_20", "top_50"]:
                assert concentration[threshold]["percentage"] <= 100
            
            # Property: monotonicity invariants
            assert concentration["top_20"]["count"] >= concentration["top_10"]["count"]
            assert concentration["top_50"]["count"] >= concentration["top_20"]["count"]
    
    def test_performance_smoke_10k_entities(self):
        """Performance smoke test with 10k entities."""
        # Generate large dataset
        entities = [f"Entity_{i}" for i in range(10000)]
        revenues = np.random.pareto(1, 10000) * 1000  # Pareto distribution for concentration
        
        df = pd.DataFrame({
            "entity": entities,
            "revenue": revenues
        })
        
        import time
        start_time = time.time()
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        
        execution_time = time.time() - start_time
        
        # Should complete within reasonable time (< 5 seconds)
        assert execution_time < 5.0
        
        # Should produce valid results
        assert "TOTAL" in result.data
        assert "error" not in result.data["TOTAL"]
        assert result.data["TOTAL"]["total_entities"] == 10000
        
        # Head sample should be limited appropriately
        head_sample = result.data["TOTAL"]["head_sample"]
        assert len(head_sample) <= 20
    
    def test_custom_thresholds_extreme(self):
        """Test analysis with unusual threshold values."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C", "D"],
            "revenue": [1000, 100, 50, 10]  # Total: 1160, A = 86.2%
        })
        
        # Test extreme thresholds
        extreme_thresholds = [1, 99]
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=extreme_thresholds)
        
        concentration = result.data["TOTAL"]["concentration"]
        
        # Should have both thresholds
        assert "top_1" in concentration
        assert "top_99" in concentration
        
        # Top 1% should include at least 1 entity (A alone is 86.2% > 1%)
        top_1 = concentration["top_1"]
        assert top_1["count"] == 1  # At least 1 entity rule
        assert top_1["value"] == 1000.0
        
        # Top 99% should include A+B (94.8% <= 99%), but not A+B+C (99.1% > 99%)
        top_99 = concentration["top_99"]
        assert top_99["count"] == 2  # Should include A, B (cumulative 94.8%)
    
    def test_single_entity_all_thresholds(self):
        """Test that single entity appears in all thresholds at 100%."""
        df = pd.DataFrame({
            "entity": ["A"],
            "revenue": [500]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue", thresholds=[5, 25, 75])
        concentration = result.data["TOTAL"]["concentration"]
        
        # Single entity should be 100% for all thresholds
        for threshold in ["top_5", "top_25", "top_75"]:
            assert concentration[threshold]["count"] == 1
            assert concentration[threshold]["value"] == 500.0
            assert concentration[threshold]["percentage"] == 100.0
            assert concentration[threshold]["entities"] == ["A"]
    
    def test_json_schema_consistency(self):
        """Test that output maintains consistent JSON schema structure."""
        df = pd.DataFrame({
            "entity": ["A", "B", "C"],
            "revenue": [100, 50, 25]
        })
        
        result = self.analyzer.analyze(df, "entity", "revenue")
        data = result.data["TOTAL"]
        
        # Required fields should always be present
        required_fields = ["period", "total_entities", "total_value", "concentration", "head_sample"]
        for field in required_fields:
            assert field in data
        
        # Concentration should have threshold keys
        concentration = data["concentration"]
        expected_thresholds = ["top_10", "top_20", "top_50"]
        for threshold in expected_thresholds:
            assert threshold in concentration
            
            # Each threshold should have required subfields
            threshold_data = concentration[threshold]
            assert "count" in threshold_data
            assert "value" in threshold_data
            assert "percentage" in threshold_data
            assert "entities" in threshold_data
            
            # Data types should be correct
            assert isinstance(threshold_data["count"], int)
            assert isinstance(threshold_data["value"], float)
            assert isinstance(threshold_data["percentage"], float)
            assert isinstance(threshold_data["entities"], list)