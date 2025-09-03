"""
LLM Executors

High-level execution functions that coordinate context preparation,
LLM invocation, validation, and persistence with full error handling.
"""

import json
import asyncio
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime, UTC

from config.settings import settings
from services.llm_client import llm_client, LLMUsageError, LLMValidationError, NotConfiguredError
from services.registry import DatasetRegistry
from core.llm.types import (
    LLM_FUNCTION_MODELS, LLMStatus, LLMArtifact,
    SchemaDescription, NarrativeInsights, RiskFlags,
    DataQualityReport, ThresholdRecommendations, QAOverContext
)
from core.llm.prompt_builders import PROMPT_BUILDERS


class LLMExecutionError(Exception):
    """Base exception for LLM execution errors."""
    pass


class LLMExecutor:
    """
    High-level LLM execution coordinator.
    
    Handles the full lifecycle:
    - Context preparation and validation
    - LLM invocation with error handling  
    - Response validation and fallbacks
    - Artifact persistence via registry
    """
    
    def __init__(self, registry: Optional[DatasetRegistry] = None):
        self.registry = registry or DatasetRegistry()
    
    async def _execute_llm_function(
        self,
        function_name: str,
        dataset_id: str,
        context: Dict[str, Any],
        request_id: Optional[str] = None,
        model: Optional[str] = None,
        **prompt_kwargs
    ) -> Tuple[Dict[str, Any], LLMStatus]:
        """
        Execute an LLM function with full error handling and persistence.
        
        Args:
            function_name: Name of the LLM function
            dataset_id: Dataset identifier
            context: Context data for the function
            request_id: Request ID for tracing
            model: LLM model to use
            **prompt_kwargs: Additional arguments for prompt building
            
        Returns:
            Tuple of (result_dict, llm_status)
        """
        # Check if LLM is disabled
        if not settings.use_llm:
            fallback_result = self._get_fallback_response(function_name)
            status = LLMStatus(used=False, reason="disabled")
            return fallback_result, status
        
        # Get prompt builder and response model
        prompt_builder = PROMPT_BUILDERS.get(function_name)
        response_model = LLM_FUNCTION_MODELS.get(function_name)
        
        if not prompt_builder or not response_model:
            raise LLMExecutionError(f"Unknown LLM function: {function_name}")
        
        start_time = datetime.now(UTC)
        
        try:
            # Build prompt messages
            messages = prompt_builder(context, **prompt_kwargs)
            
            # Execute LLM request
            response_json, metrics = await llm_client.chat_json(
                messages=messages,
                response_model=response_model,
                model=model,
                request_id=request_id,
                dataset_id=dataset_id,
                function_name=function_name,
                context=context
            )
            
            # Create artifact for persistence
            artifact = LLMArtifact(
                function_name=function_name,
                request_id=request_id,
                dataset_id=dataset_id,
                timestamp=start_time.isoformat(),
                model=metrics.model,
                provider=metrics.provider,
                context_hash=llm_client._generate_context_hash(context),
                response=response_json,
                latency_ms=metrics.latency_ms,
                usage=metrics.usage,
                cached=metrics.cached,
                error=metrics.error
            )
            
            # Persist artifact
            artifact_path = self.registry.record_llm_artifact(
                dataset_id=dataset_id,
                artifact_name=f"{function_name}_{int(start_time.timestamp())}",
                content=artifact.model_dump()
            )
            
            # Create status
            status = LLMStatus(
                used=True,
                model=metrics.model,
                latency_ms=metrics.latency_ms,
                cached=metrics.cached
            )
            
            return response_json, status
            
        except NotConfiguredError as e:
            # Provider not configured
            fallback_result = self._get_fallback_response(function_name)
            status = LLMStatus(used=False, reason="not_configured")
            return fallback_result, status
            
        except LLMUsageError as e:
            # Usage limit exceeded
            fallback_result = self._get_fallback_response(function_name)
            status = LLMStatus(used=False, reason=f"usage_limit: {str(e)}")
            return fallback_result, status
            
        except LLMValidationError as e:
            # Response validation failed - return fallback
            fallback_result = self._get_fallback_response(function_name)
            status = LLMStatus(used=False, reason=f"validation_error: {str(e)}")
            
            # Still persist the failed attempt for debugging
            artifact = LLMArtifact(
                function_name=function_name,
                request_id=request_id,
                dataset_id=dataset_id,
                timestamp=start_time.isoformat(),
                model=model or settings.llm_model,
                provider=settings.llm_provider or "openai",
                context_hash=llm_client._generate_context_hash(context),
                response=fallback_result,
                latency_ms=0,
                error=str(e)
            )
            
            self.registry.record_llm_artifact(
                dataset_id=dataset_id,
                artifact_name=f"{function_name}_failed_{int(start_time.timestamp())}",
                content=artifact.model_dump()
            )
            
            return fallback_result, status
            
        except Exception as e:
            # Unexpected error - return fallback
            fallback_result = self._get_fallback_response(function_name)
            status = LLMStatus(used=False, reason=f"error: {str(e)}")
            return fallback_result, status
    
    def _get_fallback_response(self, function_name: str) -> Dict[str, Any]:
        """Get a conservative fallback response for when LLM fails."""
        fallbacks = {
            "schema_description": {
                "column_descriptions": {},
                "business_context": "Unable to generate schema description at this time.",
                "data_quality_notes": [],
                "recommended_analyses": [],
                "confidence_notes": ["This is a fallback response - LLM analysis unavailable"]
            },
            "narrative_insights": {
                "executive_summary": "Concentration analysis completed. Detailed insights unavailable at this time.",
                "key_findings": ["Analysis complete - see concentration metrics for details"],
                "risk_indicators": [],
                "opportunities": [],
                "recommendations": ["Review concentration metrics for business insights"],
                "confidence_notes": ["This is a fallback response - LLM analysis unavailable"]
            },
            "risk_flags": {
                "level": "medium",
                "reasons": ["Unable to assess risk level - please review concentration metrics manually"]
            },
            "data_quality_report": {
                "issues": [],
                "recommendations": ["Review dataset for quality issues manually"],
                "severity_score": None
            },
            "threshold_recommendations": {
                "suggested": [10, 20, 50],
                "rationale": "Using default industry-standard thresholds"
            },
            "qa_over_context": {
                "answer": "Unable to process question at this time. Please refer to the raw data.",
                "citations": [],
                "confidence": "low"
            }
        }
        
        return fallbacks.get(function_name, {"error": "Unknown function"})
    
    # Individual function executors
    
    async def generate_schema_description(
        self,
        dataset_id: str,
        schema: Dict[str, Any],
        dataset_stats: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[SchemaDescription, LLMStatus]:
        """Generate schema description with business context."""
        result, status = await self._execute_llm_function(
            function_name="schema_description",
            dataset_id=dataset_id,
            context={"schema": schema, "dataset_stats": dataset_stats or {}},
            request_id=request_id,
            model=model
        )
        
        return SchemaDescription(**result), status
    
    async def generate_narrative_insights(
        self,
        dataset_id: str,
        concentration_results: Dict[str, Any],
        schema: Dict[str, Any],
        thresholds: List[int],
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[NarrativeInsights, LLMStatus]:
        """Generate narrative insights from concentration analysis."""
        result, status = await self._execute_llm_function(
            function_name="narrative_insights",
            dataset_id=dataset_id,
            context={
                "analysis": concentration_results,
                "schema": schema,
                "thresholds": thresholds
            },
            request_id=request_id,
            model=model
        )
        
        return NarrativeInsights(**result), status
    
    async def generate_risk_flags(
        self,
        dataset_id: str,
        concentration_results: Dict[str, Any],
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[RiskFlags, LLMStatus]:
        """Generate risk assessment from concentration data."""
        result, status = await self._execute_llm_function(
            function_name="risk_flags",
            dataset_id=dataset_id,
            context={"concentration_analysis": concentration_results},
            request_id=request_id,
            model=model
        )
        
        return RiskFlags(**result), status
    
    async def generate_data_quality_report(
        self,
        dataset_id: str,
        schema: Dict[str, Any],
        normalization_warnings: Optional[List[str]] = None,
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[DataQualityReport, LLMStatus]:
        """Generate data quality assessment report."""
        result, status = await self._execute_llm_function(
            function_name="data_quality_report",
            dataset_id=dataset_id,
            context={
                "schema": schema,
                "normalization_warnings": normalization_warnings or []
            },
            request_id=request_id,
            model=model
        )
        
        return DataQualityReport(**result), status
    
    async def generate_threshold_recommendations(
        self,
        dataset_id: str,
        concentration_results: Dict[str, Any],
        current_thresholds: List[int],
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[ThresholdRecommendations, LLMStatus]:
        """Generate threshold recommendations for concentration analysis."""
        result, status = await self._execute_llm_function(
            function_name="threshold_recommendations", 
            dataset_id=dataset_id,
            context={
                "concentration_analysis": concentration_results,
                "current_thresholds": current_thresholds
            },
            request_id=request_id,
            model=model
        )
        
        return ThresholdRecommendations(**result), status
    
    async def answer_question(
        self,
        dataset_id: str,
        user_question: str,
        context: Dict[str, Any],
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Tuple[QAOverContext, LLMStatus]:
        """Answer user question based on provided context."""
        # Note: user_question gets passed as prompt_kwarg, not in context
        result, status = await self._execute_llm_function(
            function_name="qa_over_context",
            dataset_id=dataset_id,
            context=context,
            request_id=request_id,
            model=model,
            user_question=user_question  # This goes to prompt builder
        )
        
        return QAOverContext(**result), status
    
    # Convenience methods for common use cases
    
    async def generate_full_insights(
        self,
        dataset_id: str,
        concentration_results: Dict[str, Any],
        schema: Dict[str, Any],
        thresholds: List[int],
        request_id: Optional[str] = None,
        model: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate all insights for a dataset in one call.
        
        Returns a comprehensive insights dictionary with all LLM outputs
        and their respective statuses.
        """
        # Run all insights in parallel for efficiency
        tasks = [
            self.generate_narrative_insights(dataset_id, concentration_results, schema, thresholds, request_id, model),
            self.generate_risk_flags(dataset_id, concentration_results, request_id, model),
            self.generate_threshold_recommendations(dataset_id, concentration_results, thresholds, request_id, model)
        ]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            narrative, narrative_status = results[0] if not isinstance(results[0], Exception) else (None, LLMStatus(used=False, reason="error"))
            risk_flags, risk_status = results[1] if not isinstance(results[1], Exception) else (None, LLMStatus(used=False, reason="error"))
            thresholds, threshold_status = results[2] if not isinstance(results[2], Exception) else (None, LLMStatus(used=False, reason="error"))
            
            return {
                "narrative_insights": {
                    "data": narrative.model_dump() if narrative else self._get_fallback_response("narrative_insights"),
                    "llm_status": narrative_status.model_dump()
                },
                "risk_assessment": {
                    "data": risk_flags.model_dump() if risk_flags else self._get_fallback_response("risk_flags"),
                    "llm_status": risk_status.model_dump()
                },
                "threshold_recommendations": {
                    "data": thresholds.model_dump() if thresholds else self._get_fallback_response("threshold_recommendations"),
                    "llm_status": threshold_status.model_dump()
                },
                "generated_at": datetime.now(UTC).isoformat(),
                "overall_llm_status": "success" if any(s.used for s in [narrative_status, risk_status, threshold_status]) else "fallback"
            }
            
        except Exception as e:
            # If everything fails, return fallbacks
            return {
                "narrative_insights": {
                    "data": self._get_fallback_response("narrative_insights"),
                    "llm_status": LLMStatus(used=False, reason=f"execution_error: {str(e)}").model_dump()
                },
                "risk_assessment": {
                    "data": self._get_fallback_response("risk_flags"),
                    "llm_status": LLMStatus(used=False, reason=f"execution_error: {str(e)}").model_dump()
                },
                "threshold_recommendations": {
                    "data": self._get_fallback_response("threshold_recommendations"),
                    "llm_status": LLMStatus(used=False, reason=f"execution_error: {str(e)}").model_dump()
                },
                "generated_at": datetime.now(UTC).isoformat(),
                "overall_llm_status": "error"
            }


# Singleton instance
llm_executor = LLMExecutor()