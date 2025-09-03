"""
API v1 Pydantic Models
"""

from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional, List, Dict, Any


class ErrorResponse(BaseModel):
    """Standardized error response envelope."""

    error: Literal[
        "ValidationError",
        "NotFound",
        "Conflict",
        "RateLimited",
        "PayloadTooLarge",
        "InternalError",
        "Unauthorized",
    ] = Field(description="Error type")
    message: str = Field(description="Human-readable error message")
    details: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional error details"
    )
    request_id: Optional[str] = Field(
        default=None, description="Request ID for tracking"
    )


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
        description="Concentration thresholds (1-100)",
    )
    run_llm: bool = Field(
        default=True,
        description="Automatically run LLM analysis after concentration analysis completes",
    )

    @field_validator("thresholds")
    @classmethod
    def validate_thresholds(cls, v):
        if v is not None:
            if not v:  # Empty list
                raise ValueError("Thresholds list cannot be empty")
            if len(v) > 10:
                raise ValueError("Maximum 10 thresholds allowed")
            # Sort and deduplicate thresholds
            v = sorted(set(v))
            for threshold in v:
                if threshold < 1 or threshold > 100:
                    raise ValueError("Thresholds must be between 1 and 100")
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


class LLMAnalysisRequest(BaseModel):
    """Request for LLM-only analysis on existing deterministic results."""

    force_refresh: bool = Field(
        default=False, description="Override existing LLM artifacts and regenerate"
    )
    functions: Optional[List[str]] = Field(
        default=None,
        description="Specific LLM functions to run. If None, runs all available functions",
    )

    @field_validator("functions")
    @classmethod
    def validate_functions(cls, v):
        if v is not None:
            valid_functions = {
                "narrative_insights",
                "risk_flags",
                "threshold_recommendations",
                "schema_description",
                "data_quality_report",
            }
            for func in v:
                if func not in valid_functions:
                    raise ValueError(
                        f"Invalid function '{func}'. Must be one of: {valid_functions}"
                    )
        return v


class LLMAnalysisResponse(BaseModel):
    """Response for LLM analysis endpoint."""

    dataset_id: str
    functions_executed: List[str] = Field(
        description="LLM functions that were executed"
    )
    artifacts_created: List[str] = Field(
        description="Files created in the llm/ directory"
    )
    llm_status: Dict[str, Any] = Field(
        description="Provider, model, and execution details"
    )
    warnings: List[str] = Field(default_factory=list)
    execution_time_ms: Optional[int] = None
