"""
LLM Insights Generation Module

Enhanced with real LLM integration using the new executor framework.
Provides comprehensive insights generation for concentration analysis.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import asyncio

from core.llm.executors import llm_executor
from core.llm.types import NarrativeInsights, RiskFlags, LLMStatus


@dataclass
class InsightReport:
    """Container for generated insights with LLM status."""

    insights: Dict[str, Any]
    context_used: Dict[str, Any]
    llm_metadata: Dict[str, Any]
    llm_status: Dict[str, Any]


class InsightGenerator:
    """Generates comprehensive insights from analysis results using LLM executor."""

    def __init__(self, executor=None):
        """
        Initialize with optional LLM executor.

        Args:
            executor: LLM executor for generating insights
        """
        self.executor = executor or llm_executor

    def generate_insights(
        self,
        concentration_results: Dict[str, Any],
        schema: Dict[str, Any],
        dataset_stats: Optional[Dict[str, Any]] = None,
        dataset_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> InsightReport:
        """
        Generate comprehensive insights from concentration analysis.

        Args:
            concentration_results: Results from concentration analysis
            schema: Dataset schema
            dataset_stats: Dataset statistics
            dataset_id: Dataset identifier for tracking
            request_id: Request ID for tracing

        Returns:
            InsightReport with generated insights and metadata
        """
        # Run async LLM execution in sync context
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        try:
            # Execute async insight generation
            if loop.is_running():
                # If loop is already running, create a new thread
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor_pool:
                    future = executor_pool.submit(
                        asyncio.run,
                        self._async_generate_insights(
                            concentration_results,
                            schema,
                            dataset_stats,
                            dataset_id,
                            request_id,
                        ),
                    )
                    full_insights = future.result(
                        timeout=120
                    )  # Longer timeout for comprehensive insights
            else:
                full_insights = loop.run_until_complete(
                    self._async_generate_insights(
                        concentration_results,
                        schema,
                        dataset_stats,
                        dataset_id,
                        request_id,
                    )
                )

            return InsightReport(
                insights=full_insights["insights"],
                context_used=full_insights["context_used"],
                llm_metadata=full_insights["llm_metadata"],
                llm_status=full_insights["llm_status"],
            )

        except Exception as e:
            # Fallback insights if LLM fails
            fallback_insights = {
                "narrative": {
                    "executive_summary": "Concentration analysis completed successfully.",
                    "key_findings": [
                        "Analysis complete - see concentration metrics for details"
                    ],
                    "risk_indicators": [],
                    "opportunities": [],
                    "recommendations": [
                        "Review concentration metrics for business insights"
                    ],
                    "confidence_notes": [f"LLM insight generation failed: {str(e)}"],
                },
                "risk_assessment": {
                    "level": "medium",
                    "reasons": [
                        "Unable to assess risk level - please review metrics manually"
                    ],
                },
            }

            return InsightReport(
                insights=fallback_insights,
                context_used={"concentration_results": "error", "schema": "error"},
                llm_metadata={"model": "none", "provider": "none", "error": str(e)},
                llm_status={"used": False, "reason": f"error: {str(e)}"},
            )

    async def _async_generate_insights(
        self,
        concentration_results: Dict[str, Any],
        schema: Dict[str, Any],
        dataset_stats: Optional[Dict[str, Any]],
        dataset_id: Optional[str],
        request_id: Optional[str],
    ) -> Dict[str, Any]:
        """Generate comprehensive insights using LLM executor."""
        dataset_id = dataset_id or "unknown"

        # Extract thresholds from concentration results
        thresholds = []
        if concentration_results and isinstance(concentration_results, dict):
            # Look for threshold information in the results
            for key in concentration_results.keys():
                if key.startswith("top_"):
                    try:
                        threshold = int(key.split("_")[1])
                        thresholds.append(threshold)
                    except (ValueError, IndexError):
                        continue

        # Default thresholds if none found
        if not thresholds:
            thresholds = [10, 20, 50]

        # Generate all insights in parallel
        tasks = [
            self.executor.generate_narrative_insights(
                dataset_id=dataset_id,
                concentration_results=concentration_results,
                schema=schema,
                thresholds=thresholds,
                request_id=request_id,
            ),
            self.executor.generate_risk_flags(
                dataset_id=dataset_id,
                concentration_results=concentration_results,
                request_id=request_id,
            ),
        ]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process narrative insights
            if isinstance(results[0], tuple) and len(results[0]) == 2:
                narrative, narrative_status = results[0]
                narrative_data = narrative.model_dump()
            else:
                narrative_data = self._get_fallback_narrative()
                narrative_status = LLMStatus(used=False, reason="execution_error")

            # Process risk flags
            if isinstance(results[1], tuple) and len(results[1]) == 2:
                risk_flags, risk_status = results[1]
                risk_data = risk_flags.model_dump()
            else:
                risk_data = {"level": "medium", "reasons": ["Unable to assess risk"]}
                risk_status = LLMStatus(used=False, reason="execution_error")

            # Compile comprehensive insights
            return {
                "insights": {
                    "narrative": narrative_data,
                    "risk_assessment": risk_data,
                    "generated_at": f"{request_id or 'unknown'}",
                    "data_summary": {
                        "total_periods": len(
                            concentration_results.get("by_period", [])
                        ),
                        "schema_columns": len(schema.get("columns", [])),
                        "period_grain": schema.get("period_grain", "none"),
                    },
                },
                "context_used": {
                    "concentration_metrics": bool(concentration_results),
                    "schema_context": bool(schema),
                    "dataset_stats": bool(dataset_stats),
                },
                "llm_metadata": {
                    "narrative_model": getattr(narrative_status, "model", "unknown"),
                    "risk_model": getattr(risk_status, "model", "unknown"),
                    "narrative_latency_ms": getattr(narrative_status, "latency_ms", 0),
                    "risk_latency_ms": getattr(risk_status, "latency_ms", 0),
                },
                "llm_status": {
                    "narrative_used": narrative_status.used,
                    "risk_used": risk_status.used,
                    "overall_success": narrative_status.used or risk_status.used,
                },
            }

        except Exception as e:
            # Fallback for complete failure
            return {
                "insights": {
                    "narrative": self._get_fallback_narrative(),
                    "risk_assessment": {
                        "level": "medium",
                        "reasons": ["Analysis error"],
                    },
                    "generated_at": f"{request_id or 'unknown'}",
                    "data_summary": {"error": str(e)},
                },
                "context_used": {"error": str(e)},
                "llm_metadata": {"error": str(e)},
                "llm_status": {"used": False, "reason": f"complete_failure: {str(e)}"},
            }

    def _get_fallback_narrative(self) -> Dict[str, Any]:
        """Get fallback narrative when LLM fails."""
        return {
            "executive_summary": "Concentration analysis completed successfully.",
            "key_findings": ["Concentration metrics have been calculated"],
            "risk_indicators": [],
            "opportunities": [],
            "recommendations": ["Review the concentration data for insights"],
            "confidence_notes": [
                "This is a fallback response - LLM analysis unavailable"
            ],
        }

    async def generate_narrative_only(
        self,
        concentration_results: Dict[str, Any],
        schema: Dict[str, Any],
        thresholds: List[int],
        dataset_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Tuple[NarrativeInsights, LLMStatus]:
        """
        Generate only narrative insights (async version).

        Args:
            concentration_results: Concentration analysis results
            schema: Dataset schema
            thresholds: Threshold values used
            dataset_id: Dataset identifier
            request_id: Request ID for tracing

        Returns:
            Tuple of (narrative_insights, llm_status)
        """
        return await self.executor.generate_narrative_insights(
            dataset_id=dataset_id or "unknown",
            concentration_results=concentration_results,
            schema=schema,
            thresholds=thresholds,
            request_id=request_id,
        )

    async def generate_risk_only(
        self,
        concentration_results: Dict[str, Any],
        dataset_id: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Tuple[RiskFlags, LLMStatus]:
        """
        Generate only risk flags (async version).

        Args:
            concentration_results: Concentration analysis results
            dataset_id: Dataset identifier
            request_id: Request ID for tracing

        Returns:
            Tuple of (risk_flags, llm_status)
        """
        return await self.executor.generate_risk_flags(
            dataset_id=dataset_id or "unknown",
            concentration_results=concentration_results,
            request_id=request_id,
        )
