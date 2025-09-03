"""
LLM Prompt Builders

Secure prompt construction with PII redaction, injection hardening,
and consistent formatting for all LLM functions.
"""

import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from core.llm.types import get_json_schema, LLM_FUNCTION_MODELS


class PromptSecurityError(Exception):
    """Raised when prompt construction encounters security issues."""

    pass


class PromptBuilder:
    """
    Secure prompt builder with PII redaction and injection hardening.

    Features:
    - Strong system prompts enforcing JSON-only output
    - Context sanitization and PII redaction
    - Size limits and head sample capping
    - Injection-safe prompt construction
    """

    # PII patterns to redact
    PII_PATTERNS = {
        "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        "phone": r"\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b",
        "ssn": r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
        "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
    }

    # Suspicious column patterns that might contain identifiers
    IDENTIFIER_PATTERNS = [
        r".*id$",
        r".*_id$",
        r".*identifier.*",
        r".*email.*",
        r".*phone.*",
        r".*ssn.*",
        r".*account.*",
        r".*customer_no.*",
    ]

    @classmethod
    def _redact_pii(cls, text: str) -> str:
        """Redact PII from text."""
        if not isinstance(text, str):
            return str(text)

        for pii_type, pattern in cls.PII_PATTERNS.items():
            text = re.sub(
                pattern, f"[REDACTED_{pii_type.upper()}]", text, flags=re.IGNORECASE
            )

        return text

    @classmethod
    def _sanitize_column_name(cls, column_name: str) -> str:
        """Check if column name looks like an identifier and anonymize if needed."""
        for pattern in cls.IDENTIFIER_PATTERNS:
            if re.match(pattern, column_name.lower()):
                return f"[ANONYMIZED_COLUMN_{hash(column_name) % 1000:03d}]"
        return column_name

    @classmethod
    def _sanitize_sample_data(
        cls, sample_data: List[Dict[str, Any]], max_items: int = 10
    ) -> List[Dict[str, Any]]:
        """Sanitize sample data by capping size and redacting PII."""
        if not sample_data:
            return []

        # Cap to max items
        limited_data = sample_data[:max_items]

        # Redact PII and anonymize suspicious columns
        sanitized = []
        for item in limited_data:
            sanitized_item = {}
            for key, value in item.items():
                sanitized_key = cls._sanitize_column_name(key)
                sanitized_value = (
                    cls._redact_pii(str(value)) if value is not None else None
                )
                sanitized_item[sanitized_key] = sanitized_value
            sanitized.append(sanitized_item)

        return sanitized

    @classmethod
    def _prepare_context_json(cls, context: Dict[str, Any]) -> str:
        """Prepare and sanitize context for LLM consumption."""
        # Deep copy to avoid mutating original
        sanitized_context = {}

        for key, value in context.items():
            if key == "schema" and isinstance(value, dict):
                # Sanitize schema column information
                sanitized_schema = value.copy()
                if "columns" in sanitized_schema:
                    sanitized_columns = []
                    for col in sanitized_schema["columns"]:
                        sanitized_col = col.copy()
                        sanitized_col["name"] = cls._sanitize_column_name(
                            col.get("name", "")
                        )
                        # Redact from any text fields
                        for text_field in ["original_name", "notes"]:
                            if text_field in sanitized_col:
                                sanitized_col[text_field] = cls._redact_pii(
                                    str(sanitized_col[text_field])
                                )
                        sanitized_columns.append(sanitized_col)
                    sanitized_schema["columns"] = sanitized_columns
                sanitized_context[key] = sanitized_schema

            elif key == "analysis" and isinstance(value, dict):
                # Sanitize analysis data including head samples
                sanitized_analysis = value.copy()
                if "by_period" in sanitized_analysis:
                    sanitized_periods = []
                    for period_data in sanitized_analysis["by_period"]:
                        sanitized_period = period_data.copy()
                        if "head" in sanitized_period:
                            sanitized_period["head"] = cls._sanitize_sample_data(
                                sanitized_period["head"]
                            )
                        sanitized_periods.append(sanitized_period)
                    sanitized_analysis["by_period"] = sanitized_periods
                sanitized_context[key] = sanitized_analysis

            else:
                # For other fields, just redact PII if it's a string
                if isinstance(value, str):
                    sanitized_context[key] = cls._redact_pii(value)
                elif isinstance(value, (list, dict)):
                    # For complex types, convert to string and redact
                    sanitized_context[key] = cls._redact_pii(
                        json.dumps(value, default=str)
                    )
                else:
                    sanitized_context[key] = value

        return json.dumps(sanitized_context, indent=2, default=str)

    @classmethod
    def _validate_user_question(cls, question: str) -> str:
        """Validate and sanitize user questions."""
        if not question or not isinstance(question, str):
            return ""

        # Length limit
        max_length = 500
        if len(question) > max_length:
            question = question[:max_length] + "..."

        # Remove potential injection patterns
        suspicious_patterns = [
            r"ignore\s+previous\s+instructions",
            r"system\s*:",
            r"assistant\s*:",
            r"<\s*system\s*>",
            r"```\s*system",
        ]

        for pattern in suspicious_patterns:
            if re.search(pattern, question, re.IGNORECASE):
                raise PromptSecurityError(
                    f"Potentially unsafe user input detected: {pattern}"
                )

        # Redact PII
        return cls._redact_pii(question)


# System prompts for each LLM function
SYSTEM_PROMPTS = {
    "schema_description": """You are a data schema analyst. Your task is to analyze dataset schemas and provide business context descriptions.

CRITICAL REQUIREMENTS:
1. Use ONLY the provided CONTEXT_JSON values - never compute new metrics or invent information
2. If information is missing or unclear, state "unknown" or note the limitation
3. Focus on business value and practical insights
4. Output MUST be valid JSON matching the SchemaDescription schema exactly
5. NO prose or explanation outside the JSON response

Your analysis should identify:
- Business meaning of columns based on names and metadata
- Overall dataset purpose and domain context  
- Data quality observations from provided statistics
- Recommended analysis approaches given the schema structure

Output valid JSON only.""",
    "narrative_insights": """You are an executive business analyst. Create concise insights from concentration analysis results.

CRITICAL REQUIREMENTS:
1. Use ONLY the provided concentration metrics - never compute new percentages or values
2. Reference specific numbers from the provided analysis results
3. Focus on actionable business implications
4. Keep insights concise and decision-focused
5. Output MUST be valid JSON matching the NarrativeInsights schema exactly
6. NO prose or explanation outside the JSON response

Create insights that help executives understand:
- Key concentration patterns and their business implications
- Risk factors from customer/entity concentration
- Growth opportunities identified in the data
- Specific, actionable recommendations

Output valid JSON only.""",
    "risk_flags": """You are a risk assessment specialist. Evaluate concentration risk using provided metrics only.

CRITICAL REQUIREMENTS:
1. Use ONLY the concentration percentages provided - no new calculations
2. Classify risk as low/medium/high based on common industry thresholds
3. Provide 1-3 specific reasons citing the provided data
4. Output MUST be valid JSON matching the RiskFlags schema exactly  
5. NO prose or explanation outside the JSON response

Risk level guidelines:
- HIGH: Top 10% > 80%, or single entity > 20%
- MEDIUM: Top 10% 60-80%, concentrated but manageable
- LOW: Top 10% < 60%, well-distributed

Output valid JSON only.""",
    "data_quality_report": """You are a data quality specialist. Assess data quality based on provided schema warnings and statistics.

CRITICAL REQUIREMENTS:
1. Use ONLY the schema metadata, null rates, and warnings provided
2. Identify specific quality issues from the provided information
3. Suggest practical, low-risk remediation steps
4. Output MUST be valid JSON matching the DataQualityReport schema exactly
5. NO prose or explanation outside the JSON response

Focus on:
- High null rates and missing data patterns
- Data type coercion issues and anomalies  
- Validation rule suggestions
- Process improvements for data collection

Output valid JSON only.""",
    "threshold_recommendations": """You are an analytical consultant. Recommend concentration analysis thresholds based on data distribution patterns.

CRITICAL REQUIREMENTS:
1. Use ONLY the concentration results provided - no new calculations
2. Suggest 1-5 threshold percentages based on observed patterns
3. Provide clear rationale tied to the specific data characteristics
4. Output MUST be valid JSON matching the ThresholdRecommendations schema exactly
5. NO prose or explanation outside the JSON response

Consider:
- Current concentration levels to set meaningful breakpoints
- Industry standards for concentration analysis
- Practical business decision-making needs

Output valid JSON only.""",
    "qa_over_context": """You are a data analyst assistant. Answer user questions using ONLY the provided context data.

CRITICAL REQUIREMENTS:
1. Answer ONLY based on information in the provided CONTEXT_JSON
2. If data is not available, clearly state "This information is not available in the provided data"
3. Cite specific context references for your answers
4. Be precise with numbers and facts
5. Output MUST be valid JSON matching the QAOverContext schema exactly
6. NO prose or explanation outside the JSON response

Provide:
- Direct answer to the question based on available data
- Specific citations referencing the context data used
- Appropriate confidence level based on data completeness

Output valid JSON only.""",
}


def build_schema_description_prompt(
    schema: Dict[str, Any], dataset_stats: Optional[Dict[str, Any]] = None
) -> List[Dict[str, str]]:
    """Build prompt for schema description function."""
    context = {"schema": schema, "dataset_stats": dataset_stats or {}}

    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["schema_description"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nAnalyze this dataset schema and provide business context descriptions.",
        },
    ]


def build_narrative_insights_prompt(
    concentration_results: Dict[str, Any], schema: Dict[str, Any], thresholds: List[int]
) -> List[Dict[str, str]]:
    """Build prompt for narrative insights function."""
    context = {
        "analysis": concentration_results,
        "schema": {
            "period_grain": schema.get("period_grain", "none"),
            "columns": len(schema.get("columns", [])),
            "dataset_id": schema.get("dataset_id"),
        },
        "thresholds": thresholds,
    }

    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["narrative_insights"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nGenerate executive insights from this concentration analysis.",
        },
    ]


def build_risk_flags_prompt(
    concentration_results: Dict[str, Any]
) -> List[Dict[str, str]]:
    """Build prompt for risk flags function."""
    context = {"concentration_analysis": concentration_results}
    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["risk_flags"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nAssess concentration risk level based on these metrics.",
        },
    ]


def build_data_quality_prompt(
    schema: Dict[str, Any], normalization_warnings: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """Build prompt for data quality report function."""
    context = {
        "schema": schema,
        "normalization_warnings": normalization_warnings or [],
        "timestamp": datetime.now().isoformat(),
    }

    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["data_quality_report"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nGenerate a data quality assessment report.",
        },
    ]


def build_threshold_recommendations_prompt(
    concentration_results: Dict[str, Any], current_thresholds: List[int]
) -> List[Dict[str, str]]:
    """Build prompt for threshold recommendations function."""
    context = {
        "concentration_analysis": concentration_results,
        "current_thresholds": current_thresholds,
    }

    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["threshold_recommendations"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nRecommend optimal concentration analysis thresholds.",
        },
    ]


def build_qa_prompt(
    user_question: str, context: Dict[str, Any]
) -> List[Dict[str, str]]:
    """Build prompt for Q&A over context function."""
    # Validate and sanitize user question
    safe_question = PromptBuilder._validate_user_question(user_question)

    context_json = PromptBuilder._prepare_context_json(context)

    return [
        {"role": "system", "content": SYSTEM_PROMPTS["qa_over_context"]},
        {
            "role": "user",
            "content": f"CONTEXT_JSON:\n{context_json}\n\nUser Question: {safe_question}\n\nAnswer the question based only on the provided context data.",
        },
    ]


# Function mapping for easy access
def _wrap_schema(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_schema_description_prompt(
        schema=context.get("schema", {}),
        dataset_stats=context.get("dataset_stats", {}),
    )


def _wrap_narrative(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_narrative_insights_prompt(
        concentration_results=context.get("analysis", {}),
        schema=context.get("schema", {}),
        thresholds=context.get("thresholds", []),
    )


def _wrap_risk(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_risk_flags_prompt(
        concentration_results=context.get("concentration_analysis", {}),
    )


def _wrap_dq(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_data_quality_prompt(
        schema=context.get("schema", {}),
        normalization_warnings=context.get("normalization_warnings", []),
    )


def _wrap_thresholds(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_threshold_recommendations_prompt(
        concentration_results=context.get("concentration_analysis", {}),
        current_thresholds=context.get("current_thresholds", []),
    )


def _wrap_qa(context: Dict[str, Any], **kwargs) -> List[Dict[str, str]]:
    return build_qa_prompt(
        user_question=kwargs.get("user_question", ""),
        context=context,
    )


PROMPT_BUILDERS = {
    "schema_description": _wrap_schema,
    "narrative_insights": _wrap_narrative,
    "risk_flags": _wrap_risk,
    "data_quality_report": _wrap_dq,
    "threshold_recommendations": _wrap_thresholds,
    "qa_over_context": _wrap_qa,
}
