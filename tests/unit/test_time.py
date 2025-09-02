"""
Tests for Time Detection Module
"""
import pytest
import pandas as pd
import numpy as np
from core.deterministic.time import TimeDetector


class TestTimeDetector:
    """Test time dimension detection and handling."""
    
    def setup_method(self):
        self.detector = TimeDetector()
    
    def test_date_column_detection(self):
        """Test detection of date columns."""
        df = pd.DataFrame({
            "date": ["2023-01-15", "2023-02-20", "2023-03-10"],
            "revenue": [100, 200, 150],
            "timestamp": ["2023-01-15 10:30:00", "2023-02-20 14:45:00", "2023-03-10 09:15:00"]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "date"
        assert "date" in result["time_candidates"]
        assert result["selected_time_columns"]["date"] == "date"
        assert result["derivations"]["date_column"] == "date"
        assert "date" in result["period_grain_candidates"]
    
    def test_year_month_detection(self):
        """Test detection of year and month columns."""
        df = pd.DataFrame({
            "year": [2023, 2023, 2023, 2024],
            "month": [1, 2, 3, 1],
            "sales": [100, 200, 150, 120]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "year_month"
        assert "year" in result["time_candidates"]
        assert "month" in result["time_candidates"]
        assert result["selected_time_columns"]["year"] == "year"
        assert result["selected_time_columns"]["month"] == "month"
        assert result["derivations"]["year_column"] == "year"
        assert result["derivations"]["month_column"] == "month"
    
    def test_year_quarter_detection(self):
        """Test detection of year and quarter columns."""
        df = pd.DataFrame({
            "year": [2023, 2023, 2023, 2024],
            "quarter": [1, 2, 3, 1],
            "revenue": [1000, 2000, 1500, 1200]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "year_quarter"
        assert "year" in result["time_candidates"]
        assert "quarter" in result["time_candidates"]
        assert result["derivations"]["year_column"] == "year"
        assert result["derivations"]["quarter_column"] == "quarter"
    
    def test_year_only_detection(self):
        """Test detection of year column only."""
        df = pd.DataFrame({
            "year": [2020, 2021, 2022, 2023],
            "total_sales": [10000, 12000, 15000, 18000]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "year"
        assert "year" in result["time_candidates"]
        assert result["derivations"]["year_column"] == "year"
        assert result["period_grain_candidates"] == ["year"]
    
    def test_no_time_detection(self):
        """Test graceful handling when no time columns detected."""
        df = pd.DataFrame({
            "product": ["A", "B", "C"],
            "revenue": [100, 200, 150],
            "cost": [50, 80, 60]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "none"
        assert result["time_candidates"] == []
        assert result["selected_time_columns"] == {}
        assert result["derivations"] == {}
        assert "No temporal dimension detected" in result["warnings"]
        assert result["period_grain_candidates"] == ["none"]
    
    def test_precedence_rules(self):
        """Test that precedence rules work: date > year+month > year+quarter > year."""
        # Date should win over year+month
        df = pd.DataFrame({
            "date": ["2023-01-15", "2023-02-20"],
            "year": [2023, 2023],
            "month": [1, 2],
            "revenue": [100, 200]
        })
        
        result = self.detector.detect_time_dimensions(df)
        assert result["period_grain"] == "date"
    
    def test_month_name_detection(self):
        """Test detection of month names."""
        df = pd.DataFrame({
            "year": [2023, 2023, 2023],
            "month_name": ["January", "February", "March"],
            "sales": [100, 200, 150]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "year_month"
        assert "month_name" in result["time_candidates"]
    
    def test_quarter_formats(self):
        """Test different quarter formats (1-4, Q1-Q4)."""
        df1 = pd.DataFrame({
            "year": [2023, 2023],
            "qtr": [1, 2],
            "revenue": [100, 200]
        })
        
        df2 = pd.DataFrame({
            "year": [2023, 2023],
            "quarter": ["Q1", "Q2"],
            "revenue": [100, 200]
        })
        
        result1 = self.detector.detect_time_dimensions(df1)
        result2 = self.detector.detect_time_dimensions(df2)
        
        assert result1["period_grain"] == "year_quarter"
        assert result2["period_grain"] == "year_quarter"
    
    def test_multiple_candidates_warning(self):
        """Test warnings when multiple candidates of same type exist."""
        df = pd.DataFrame({
            "date": ["2023-01-15", "2023-02-20"],
            "timestamp": ["2023-01-15 10:30:00", "2023-02-20 14:45:00"],
            "revenue": [100, 200]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert any("Multiple date columns found" in w for w in result["warnings"])
        assert result["period_grain"] == "date"  # Should still work, using first
    
    def test_year_validation(self):
        """Test year column validation (1900-2100)."""
        # Valid years
        df_valid = pd.DataFrame({
            "year": [2020, 2021, 2022],
            "sales": [100, 200, 150]
        })
        
        # Invalid years (outside range)
        df_invalid = pd.DataFrame({
            "year": [1800, 2200, 3000],
            "sales": [100, 200, 150]
        })
        
        result_valid = self.detector.detect_time_dimensions(df_valid)
        result_invalid = self.detector.detect_time_dimensions(df_invalid)
        
        assert result_valid["period_grain"] == "year"
        assert result_invalid["period_grain"] == "none"  # Should not detect invalid years
    
    def test_month_validation(self):
        """Test month column validation (1-12 or month names)."""
        # Valid months
        df_valid = pd.DataFrame({
            "year": [2023, 2023, 2023],
            "month": [1, 6, 12],
            "sales": [100, 200, 150]
        })
        
        # Invalid months
        df_invalid = pd.DataFrame({
            "year": [2023, 2023, 2023],
            "month": [13, 25, 0],
            "sales": [100, 200, 150]
        })
        
        result_valid = self.detector.detect_time_dimensions(df_valid)
        result_invalid = self.detector.detect_time_dimensions(df_invalid)
        
        assert result_valid["period_grain"] == "year_month"
        assert result_invalid["period_grain"] == "year"  # Should fall back to year only
    
    def test_quarter_validation(self):
        """Test quarter column validation (1-4, Q1-Q4)."""
        # Valid quarters
        df_valid = pd.DataFrame({
            "year": [2023, 2023],
            "quarter": [1, 4],
            "sales": [100, 200]
        })
        
        # Invalid quarters
        df_invalid = pd.DataFrame({
            "year": [2023, 2023],
            "quarter": [5, 8],
            "sales": [100, 200]
        })
        
        result_valid = self.detector.detect_time_dimensions(df_valid)
        result_invalid = self.detector.detect_time_dimensions(df_invalid)
        
        assert result_valid["period_grain"] == "year_quarter"
        assert result_invalid["period_grain"] == "year"
    
    def test_compose_period_key_date(self):
        """Test period key composition for date grain."""
        df = pd.DataFrame({
            "date": ["2023-01-15", "2023-02-20", "2023-03-10"]
        })
        
        derivations = {"date_column": "date"}
        result = self.detector.compose_period_key(df, "date", derivations)
        
        expected = ["2023-01-15", "2023-02-20", "2023-03-10"]
        assert result.tolist() == expected
    
    def test_compose_period_key_year_month(self):
        """Test period key composition for year_month grain."""
        df = pd.DataFrame({
            "year": [2023, 2023, 2024],
            "month": [1, 12, 3]
        })
        
        derivations = {"year_column": "year", "month_column": "month"}
        result = self.detector.compose_period_key(df, "year_month", derivations)
        
        expected = ["2023-M01", "2023-M12", "2024-M03"]
        assert result.tolist() == expected
    
    def test_compose_period_key_year_quarter(self):
        """Test period key composition for year_quarter grain."""
        df = pd.DataFrame({
            "year": [2023, 2023, 2024],
            "quarter": [1, 4, 2]
        })
        
        derivations = {"year_column": "year", "quarter_column": "quarter"}
        result = self.detector.compose_period_key(df, "year_quarter", derivations)
        
        expected = ["2023-Q1", "2023-Q4", "2024-Q2"]
        assert result.tolist() == expected
    
    def test_compose_period_key_year(self):
        """Test period key composition for year grain."""
        df = pd.DataFrame({
            "year": [2020, 2021, 2022]
        })
        
        derivations = {"year_column": "year"}
        result = self.detector.compose_period_key(df, "year", derivations)
        
        expected = ["2020", "2021", "2022"]
        assert result.tolist() == expected
    
    def test_compose_period_key_none(self):
        """Test period key composition for none grain."""
        df = pd.DataFrame({
            "product": ["A", "B", "C"],
            "revenue": [100, 200, 150]
        })
        
        result = self.detector.compose_period_key(df, "none", {})
        
        expected = ["ALL", "ALL", "ALL"]
        assert result.tolist() == expected
    
    def test_month_normalization(self):
        """Test month normalization to 1-12."""
        # Test with month names
        df_names = pd.DataFrame({
            "year": [2023, 2023, 2023],
            "month": ["January", "Feb", "December"]
        })
        
        derivations = {"year_column": "year", "month_column": "month"}
        result = self.detector.compose_period_key(df_names, "year_month", derivations)
        
        expected = ["2023-M01", "2023-M02", "2023-M12"]
        assert result.tolist() == expected
    
    def test_quarter_normalization(self):
        """Test quarter normalization to 1-4."""
        # Test with Q1-Q4 format
        df_q = pd.DataFrame({
            "year": [2023, 2023],
            "quarter": ["Q1", "Q4"]
        })
        
        derivations = {"year_column": "year", "quarter_column": "quarter"}
        result = self.detector.compose_period_key(df_q, "year_quarter", derivations)
        
        expected = ["2023-Q1", "2023-Q4"]
        assert result.tolist() == expected
    
    def test_invalid_data_handling(self):
        """Test handling of invalid/missing data in period key composition."""
        df = pd.DataFrame({
            "year": [2023, np.nan, 2024],
            "month": [1, 2, np.nan]
        })
        
        derivations = {"year_column": "year", "month_column": "month"}
        result = self.detector.compose_period_key(df, "year_month", derivations)
        
        # Should handle NaN values gracefully
        assert "2023-M01" in result.tolist()
        assert "UNKNOWN" in result.tolist()  # For NaN values
    
    def test_multilingual_patterns(self):
        """Test detection of time columns in different languages."""
        df = pd.DataFrame({
            "année": [2023, 2023],  # French for year
            "mois": [1, 2],         # French for month
            "revenue": [100, 200]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        # Should detect French column names
        assert result["period_grain"] == "year_month"
    
    def test_edge_case_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame()
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "none"
        assert result["time_candidates"] == []
    
    def test_edge_case_null_columns(self):
        """Test handling of columns with all null values."""
        df = pd.DataFrame({
            "year": [np.nan, np.nan, np.nan],
            "revenue": [100, 200, 150]
        })
        
        result = self.detector.detect_time_dimensions(df)
        
        assert result["period_grain"] == "none"  # Should not detect null year column