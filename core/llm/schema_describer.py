"""
LLM Schema Description Module

Enhanced with real LLM integration using the new executor framework.
"""
from typing import Dict, Any, Optional
import pandas as pd
import asyncio

from core.llm.executors import llm_executor
from core.llm.types import SchemaDescription, LLMStatus


class SchemaDescriber:
    """Generates LLM-enhanced schema descriptions using the executor framework."""
    
    def __init__(self, executor=None):
        """
        Initialize with optional LLM executor.
        
        Args:
            executor: LLM executor for generating descriptions
        """
        self.executor = executor or llm_executor
    
    def enhance_schema(
        self,
        base_schema: Dict[str, Any],
        sample_data: Optional[pd.DataFrame] = None,
        dataset_id: Optional[str] = None,
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Enhance schema with LLM descriptions.
        
        Args:
            base_schema: Deterministic schema
            sample_data: Sample of the data (optional)
            dataset_id: Dataset identifier for tracking
            request_id: Request ID for tracing
        
        Returns:
            Enhanced schema with LLM insights
        """
        # Run async LLM execution in sync context
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Prepare dataset stats from sample data if available
            dataset_stats = {}
            if sample_data is not None:
                dataset_stats = {
                    "row_count": len(sample_data),
                    "column_count": len(sample_data.columns),
                    "memory_usage_mb": round(sample_data.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                }
            
            # Execute LLM description generation
            if loop.is_running():
                # If loop is already running, create a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._async_enhance_schema(
                            base_schema, dataset_stats, dataset_id, request_id
                        )
                    )
                    description, llm_status = future.result(timeout=60)
            else:
                description, llm_status = loop.run_until_complete(
                    self._async_enhance_schema(base_schema, dataset_stats, dataset_id, request_id)
                )
            
            # Enhance schema with LLM results
            enhanced_schema = base_schema.copy()
            enhanced_schema["llm_insights"] = description.model_dump()
            enhanced_schema["llm_status"] = llm_status.model_dump()
            
            return enhanced_schema
            
        except Exception as e:
            # Fallback to basic schema if LLM fails
            enhanced_schema = base_schema.copy()
            enhanced_schema["llm_insights"] = {
                "column_descriptions": {},
                "business_context": "Schema description unavailable",
                "data_quality_notes": [],
                "recommended_analyses": [],
                "confidence_notes": [f"LLM enhancement failed: {str(e)}"]
            }
            enhanced_schema["llm_status"] = {
                "used": False,
                "reason": f"error: {str(e)}"
            }
            return enhanced_schema
    
    async def _async_enhance_schema(
        self,
        base_schema: Dict[str, Any],
        dataset_stats: Dict[str, Any],
        dataset_id: Optional[str],
        request_id: Optional[str]
    ) -> tuple[SchemaDescription, LLMStatus]:
        """Async wrapper for schema enhancement."""
        return await self.executor.generate_schema_description(
            dataset_id=dataset_id or "unknown",
            schema=base_schema,
            dataset_stats=dataset_stats,
            request_id=request_id
        )
    
    def enhance_schema_sync(
        self,
        base_schema: Dict[str, Any],
        dataset_id: str,
        dataset_stats: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Synchronous version for use in sync contexts.
        
        Args:
            base_schema: Deterministic schema
            dataset_id: Dataset identifier
            dataset_stats: Optional dataset statistics
            request_id: Request ID for tracing
            
        Returns:
            Enhanced schema with LLM insights
        """
        return self.enhance_schema(
            base_schema=base_schema,
            sample_data=None,
            dataset_id=dataset_id,
            request_id=request_id
        )