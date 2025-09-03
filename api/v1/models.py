"""
API v1 Pydantic Models
"""

from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional, List, Dict, Any
from datetime import datetime


class UploadResponse(BaseModel):
    """Response for file upload endpoint."""

    dataset_id: str
    status: str = Field(description="Processing status")
    message: Optional[str] = None
    rows_processed: Optional[int] = None
    columns_processed: Optional[int] = None


class ColumnInfo(BaseModel):
    """Schema column information."""

    name: str
    original_name: str
    dtype: str
    role: Literal["categorical", "numeric", "datetime"] = Field(
        description="Column role: categorical | numeric | datetime"
    )
    cardinality: int
    null_rate: float
    notes: List[Dict[str, Any]] = Field(default_factory=list)


class SchemaResponse(BaseModel):
    """Response for schema endpoint."""

    dataset_id: str
    columns: List[ColumnInfo]
    period_grain: str = Field(description="year_month|year_quarter|year|none")
    period_grain_candidates: List[str] = Field(description="Possible period grains")
    time_candidates: List[str]
    warnings: List[str] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)
    llm_insights: Optional[Dict[str, Any]] = None


class ConcentrationRequest(BaseModel):
    """Request for concentration analysis."""

    group_by: str = Field(description="Column to group by", min_length=1)
    value: str = Field(description="Column to aggregate", min_length=1)
    thresholds: Optional[List[int]] = Field(
        default_factory=lambda: [10, 20, 50], 
        description="Concentration thresholds (1-99)"
    )
    
    @field_validator('thresholds')
    @classmethod
    def validate_thresholds(cls, v):
        if v is not None:
            if not v:  # Empty list
                raise ValueError("Thresholds list cannot be empty")
            if len(v) > 10:
                raise ValueError("Maximum 10 thresholds allowed")
            for threshold in v:
                if threshold < 1 or threshold > 99:
                    raise ValueError("Thresholds must be between 1 and 99")
            if len(set(v)) != len(v):
                raise ValueError("Duplicate thresholds not allowed")
        return v


class ConcentrationMetrics(BaseModel):
    """Metrics for a concentration threshold."""

    count: int
    value: float
    pct_of_total: float


class PeriodConcentration(BaseModel):
    """Concentration results for a time period."""

    period: str
    total: float
    concentration: Dict[str, ConcentrationMetrics] = Field(default_factory=dict)
    head: List[Dict[str, Any]] = Field(default_factory=list)


class ConcentrationResponse(BaseModel):
    """Response for concentration analysis."""

    dataset_id: str
    period_grain: str
    warnings: List[str]
    thresholds: List[int]
    by_period: List[PeriodConcentration]
    totals: Dict[str, Any]
    export_links: Optional[Dict[str, str]] = None


class InsightsResponse(BaseModel):
    """Response for insights endpoint."""

    dataset_id: str
    executive_summary: str
    key_findings: List[str]
    risk_indicators: List[str]
    opportunities: List[str]
    recommendations: List[str]
    confidence_notes: Optional[List[str]] = None
