"""
LLM Type Definitions

Comprehensive Pydantic models for all LLM function contracts.
Ensures structured JSON outputs with strict validation.
"""

from typing import List, Dict, Any, Optional, Literal
from enum import Enum
from pydantic import BaseModel, Field, validator, ConfigDict


class LLMStatus(BaseModel):
    """Status information for LLM operations."""
    used: bool = Field(description="Whether LLM was actually used")
    reason: Optional[str] = Field(default=None, description="Reason for status (e.g., 'disabled', 'cached', 'error')")
    model: Optional[str] = Field(default=None, description="Model used for generation")
    latency_ms: Optional[int] = Field(default=None, description="Request latency in milliseconds")
    cached: bool = Field(default=False, description="Whether response was served from cache")
    

class SchemaDescription(BaseModel):
    """Schema enhancement with business context and quality notes."""
    column_descriptions: Dict[str, str] = Field(
        description="Mapping of column names to business descriptions",
        examples=[{"customer_id": "Unique identifier for customer accounts"}]
    )
    business_context: str = Field(
        description="Overall business context and purpose of the dataset",
        examples=["Customer transaction data for retail analysis"]
    )
    data_quality_notes: List[str] = Field(
        description="Observations about data quality issues or anomalies",
        examples=["High null rate in optional_field column", "Some negative values in revenue column"]
    )
    recommended_analyses: List[str] = Field(
        description="Suggested analytical approaches based on schema",
        examples=["Time series analysis on transaction dates", "Customer segmentation by transaction volume"]
    )
    confidence_notes: List[str] = Field(
        default_factory=list,
        description="Notes about confidence level of descriptions due to sparse data"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "column_descriptions": {
                    "customer_id": "Unique customer identifier",
                    "transaction_amount": "Purchase amount in local currency"
                },
                "business_context": "E-commerce transaction data for customer behavior analysis",
                "data_quality_notes": ["High data completeness", "Some outliers in transaction amounts"],
                "recommended_analyses": ["Customer lifetime value", "Seasonal purchase patterns"],
                "confidence_notes": []
            }
        }
    )


class NarrativeInsights(BaseModel):
    """Narrative insights from concentration analysis."""
    executive_summary: str = Field(
        description="High-level summary of key findings for executives",
        min_length=10,
        max_length=500
    )
    key_findings: List[str] = Field(
        description="Specific quantitative findings from the analysis",
        min_length=1,
        max_length=10
    )
    risk_indicators: List[str] = Field(
        description="Potential risks identified from concentration patterns",
        max_length=5
    )
    opportunities: List[str] = Field(
        description="Business opportunities suggested by the data",
        max_length=5
    )
    recommendations: List[str] = Field(
        description="Actionable recommendations based on findings",
        min_length=1,
        max_length=5
    )
    confidence_notes: List[str] = Field(
        default_factory=list,
        description="Notes about confidence level due to data limitations"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "executive_summary": "Analysis reveals high customer concentration with top 10% accounting for 80% of revenue, indicating significant dependency risk.",
                "key_findings": [
                    "Top 10% of customers generate 80% of total revenue",
                    "Customer concentration increased 15% year-over-year"
                ],
                "risk_indicators": [
                    "High dependency on few large customers",
                    "Potential revenue volatility from customer churn"
                ],
                "opportunities": [
                    "Expand mid-tier customer segment",
                    "Develop retention programs for top customers"
                ],
                "recommendations": [
                    "Diversify customer base through targeted acquisition",
                    "Implement tiered loyalty programs"
                ],
                "confidence_notes": []
            }
        }
    )


class RiskLevel(str, Enum):
    """Risk level enumeration."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class RiskFlags(BaseModel):
    """Risk assessment based on concentration metrics."""
    level: RiskLevel = Field(description="Overall risk level assessment")
    reasons: List[str] = Field(
        description="Specific reasons for the risk level",
        min_length=1,
        max_length=5
    )
    score: Optional[float] = Field(
        default=None,
        description="Numerical risk score (0-100)",
        ge=0,
        le=100
    )

    @validator('reasons')
    def validate_reasons(cls, v):
        if not v:
            raise ValueError("At least one reason must be provided")
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "level": "high",
                "reasons": [
                    "Top 10% concentration exceeds 80% threshold",
                    "Single customer represents >15% of total value"
                ],
                "score": 85.5
            }
        }
    )


class DataQualityReport(BaseModel):
    """Data quality assessment and recommendations."""
    issues: List[str] = Field(
        description="Identified data quality issues",
        max_length=10
    )
    recommendations: List[str] = Field(
        description="Recommended actions to improve data quality",
        max_length=10
    )
    severity_score: Optional[int] = Field(
        default=None,
        description="Overall severity score (1-10, where 10 is most severe)",
        ge=1,
        le=10
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "issues": [
                    "15% null rate in customer_category column",
                    "Negative values detected in revenue column",
                    "Inconsistent date formats in transaction_date"
                ],
                "recommendations": [
                    "Implement validation rules for revenue column",
                    "Standardize date input formats",
                    "Review customer_category data collection process"
                ],
                "severity_score": 6
            }
        }
    )


class ThresholdRecommendations(BaseModel):
    """Recommended concentration thresholds with rationale."""
    suggested: List[int] = Field(
        description="Suggested threshold percentages",
        min_length=1,
        max_length=5
    )
    rationale: str = Field(
        description="Explanation for the suggested thresholds",
        min_length=10,
        max_length=300
    )

    @validator('suggested')
    def validate_thresholds(cls, v):
        if not all(1 <= x <= 100 for x in v):
            raise ValueError("Thresholds must be between 1 and 100")
        if len(set(v)) != len(v):
            raise ValueError("Thresholds must be unique")
        return sorted(v)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "suggested": [10, 25, 50],
                "rationale": "Given the high concentration patterns, recommend 10% for top performers, 25% for key segment, and 50% for majority analysis."
            }
        }
    )


class QAOverContext(BaseModel):
    """Question answering over provided context."""
    answer: str = Field(
        description="Answer to the user's question based on available context",
        min_length=1,
        max_length=500
    )
    citations: List[str] = Field(
        description="Specific references to context data that support the answer",
        max_length=5
    )
    confidence: Optional[str] = Field(
        default=None,
        description="Confidence level in the answer (high/medium/low)"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "answer": "Top 10% of customers account for 80.5% of total revenue based on the concentration analysis.",
                "citations": ["by_period[0].concentration.top_10.pct_of_total"],
                "confidence": "high"
            }
        }
    )


class LLMArtifact(BaseModel):
    """Container for LLM artifacts with full audit trail."""
    function_name: str = Field(description="Name of the LLM function")
    request_id: Optional[str] = Field(description="Request ID for tracing")
    dataset_id: str = Field(description="Dataset identifier")
    timestamp: str = Field(description="ISO timestamp of request")
    
    # Request details
    model: str = Field(description="Model used")
    provider: str = Field(description="LLM provider")
    context_hash: str = Field(description="Hash of input context")
    
    # Response details
    response: Dict[str, Any] = Field(description="LLM response data")
    latency_ms: int = Field(description="Request latency")
    usage: Optional[Dict[str, Any]] = Field(default=None, description="Token usage")
    cached: bool = Field(default=False, description="Whether response was cached")
    error: Optional[str] = Field(default=None, description="Error message if failed")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "function_name": "narrative_insights",
                "request_id": "req_abc123",
                "dataset_id": "ds_xyz789",
                "timestamp": "2023-12-01T10:30:00Z",
                "model": "gpt-4.1-mini",
                "provider": "openai",
                "context_hash": "a1b2c3d4",
                "response": {"executive_summary": "..."},
                "latency_ms": 1250,
                "usage": {"prompt_tokens": 500, "completion_tokens": 200},
                "cached": False,
                "error": None
            }
        }
    )


# JSON Schema Generation Helper
def get_json_schema(model_class: type) -> Dict[str, Any]:
    """Get JSON schema for a Pydantic model."""
    return model_class.model_json_schema()


# All available LLM function models
LLM_FUNCTION_MODELS = {
    "schema_description": SchemaDescription,
    "narrative_insights": NarrativeInsights,
    "risk_flags": RiskFlags,
    "data_quality_report": DataQualityReport,
    "threshold_recommendations": ThresholdRecommendations,
    "qa_over_context": QAOverContext
}


# Export schemas for reference
def export_schemas() -> Dict[str, Dict[str, Any]]:
    """Export all schemas for documentation."""
    return {
        name: get_json_schema(model)
        for name, model in LLM_FUNCTION_MODELS.items()
    }