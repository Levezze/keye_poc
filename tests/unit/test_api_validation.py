"""
Test API Request Validation
"""
import pytest
from pydantic import ValidationError
from api.v1.models import ConcentrationRequest


class TestAPIValidation:
    """Test API request validation."""
    
    def test_concentration_request_valid(self):
        """Test valid concentration request."""
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue",
            thresholds=[10, 20, 50]
        )
        assert request.group_by == "entity"
        assert request.value == "revenue"
        assert request.thresholds == [10, 20, 50]
    
    def test_concentration_request_defaults(self):
        """Test concentration request with defaults."""
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue"
        )
        assert request.thresholds == [10, 20, 50]
    
    def test_empty_group_by_field(self):
        """Test validation fails for empty group_by."""
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="",
                value="revenue"
            )
        assert "at least 1 character" in str(exc_info.value)
    
    def test_empty_value_field(self):
        """Test validation fails for empty value field."""
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value=""
            )
        assert "at least 1 character" in str(exc_info.value)
    
    def test_empty_thresholds_list(self):
        """Test validation fails for empty thresholds list."""
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value="revenue",
                thresholds=[]
            )
        assert "cannot be empty" in str(exc_info.value)
    
    def test_invalid_threshold_range(self):
        """Test validation fails for thresholds outside 1-100 range."""
        # Test threshold too low
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value="revenue",
                thresholds=[0, 20, 50]
            )
        assert "must be between 1 and 100" in str(exc_info.value)
        
        # Test threshold too high
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value="revenue",
                thresholds=[10, 101, 50]
            )
        assert "must be between 1 and 100" in str(exc_info.value)
    
    def test_duplicate_thresholds_deduplication(self):
        """Test that duplicate thresholds are automatically deduplicated and sorted."""
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue",
            thresholds=[20, 10, 20, 10]
        )
        # Should be deduplicated and sorted: [10, 20]
        assert request.thresholds == [10, 20]
    
    def test_too_many_thresholds(self):
        """Test validation fails for too many thresholds."""
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value="revenue",
                thresholds=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
            )
        assert "Maximum 10 thresholds" in str(exc_info.value)
    
    def test_non_integer_thresholds(self):
        """Test validation fails for non-integer thresholds."""
        with pytest.raises(ValidationError) as exc_info:
            ConcentrationRequest(
                group_by="entity",
                value="revenue",
                thresholds=[10.5, 20, 50]
            )
        assert "valid integer" in str(exc_info.value)
    
    def test_custom_thresholds_valid(self):
        """Test various valid custom threshold combinations."""
        # Single threshold
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue",
            thresholds=[75]
        )
        assert request.thresholds == [75]
        
        # Extreme values
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue",
            thresholds=[1, 99]
        )
        assert request.thresholds == [1, 99]
        
        # Many thresholds (at limit)
        request = ConcentrationRequest(
            group_by="entity",
            value="revenue",
            thresholds=[1, 5, 10, 15, 20, 25, 50, 75, 90, 99]
        )
        assert len(request.thresholds) == 10