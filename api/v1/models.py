"""
API v1 Pydantic Models
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class UploadResponse(BaseModel):
    """Response for file upload endpoint."""
    dataset_id: str
    status: str = Field(description="Processing status")
    message: Optional[str] = None


class ColumnInfo(BaseModel):
    """Schema column information."""
    name: str
    original_name: str
    dtype: str
    role: str = Field(description="Column role: dimension, measure, time")
    cardinality: int
    null_rate: float
    notes: List[Dict[str, Any]] = []


class SchemaResponse(BaseModel):
    """Response for schema endpoint."""
    dataset_id: str
    columns: List[ColumnInfo]
    period_grain: str = Field(description="year_month|year_quarter|year|none")
    time_candidates: List[str]
    notes: List[str] = []
    llm_insights: Optional[Dict[str, Any]] = None


class ConcentrationRequest(BaseModel):
    """Request for concentration analysis."""
    group_by: str = Field(description="Column to group by")
    value: str = Field(description="Column to aggregate")
    time: Optional[str] = Field(None, description="Time column to use")
    thresholds: Optional[List[int]] = Field([10, 20, 50], description="Concentration thresholds")


class ConcentrationMetrics(BaseModel):
    """Metrics for a concentration threshold."""
    count: int
    value: float
    pct_of_total: float


class PeriodConcentration(BaseModel):
    """Concentration results for a time period."""
    period: str
    total: float
    top_10: Optional[ConcentrationMetrics] = None
    top_20: Optional[ConcentrationMetrics] = None
    top_50: Optional[ConcentrationMetrics] = None
    head: List[Dict[str, Any]] = []


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