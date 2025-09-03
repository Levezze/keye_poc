"""
Provider-Agnostic LLM Client Service

Unified client using OpenAI SDK with provider-specific base_url overrides.
Supports OpenAI, Anthropic Claude, and Google Gemini with comprehensive
security, reliability, and cost controls.
"""

import hashlib
import json
import time
from datetime import datetime, UTC
from typing import Dict, Any, Optional, List, Tuple
import asyncio
from functools import wraps

import openai
from openai import OpenAI
from pydantic import BaseModel, ValidationError

from config.settings import settings


class LLMUsageError(Exception):
    """Raised when LLM usage limits are exceeded."""

    pass


class LLMValidationError(Exception):
    """Raised when LLM response validation fails."""

    pass


class LLMCacheEntry(BaseModel):
    """Cache entry for LLM responses."""

    response: Dict[str, Any]
    timestamp: float
    model: str
    provider: str
    usage: Optional[Dict[str, Any]] = None


class LLMRequestMetrics(BaseModel):
    """Metrics for an LLM request."""

    request_id: Optional[str] = None
    provider: str
    model: str
    latency_ms: int
    retry_count: int = 0
    cached: bool = False
    usage: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


def retry_with_backoff(max_retries: int = 2, base_delay: float = 1.0):
    """Decorator for retry logic with exponential backoff."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (openai.RateLimitError, openai.APIError) as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        await asyncio.sleep(delay)
                        continue
                    else:
                        raise
                except Exception as e:
                    # Don't retry on validation errors, auth errors, etc.
                    raise e
            raise last_exception

        return wrapper

    return decorator


class LLMClient:
    """
    Provider-agnostic LLM client with security and reliability features.

    Features:
    - Multiple providers via base_url overrides
    - Comprehensive caching with TTL
    - Cost controls and usage tracking
    - Retry logic with exponential backoff
    - Request validation and security hardening
    - Full audit trail
    """

    # Allowed base URLs for security (no arbitrary endpoints)
    ALLOWED_BASE_URLS = {
        "openai": "https://api.openai.com/v1/",
        "anthropic": "https://api.anthropic.com/v1/",
        "gemini": "https://generativelanguage.googleapis.com/v1beta/",
    }

    # Provider-specific model mappings
    MODEL_MAPPINGS = {
        "openai": {
            "gpt-4.1-mini": "gpt-4o-mini",
            "gpt-4.1": "gpt-4o",
            "gpt-3.5": "gpt-3.5-turbo",
        },
        "anthropic": {
            "claude-3.5-haiku": "claude-3-5-haiku-20241022",
            "claude-3.5-sonnet": "claude-3-5-sonnet-20241022",
            "claude-3": "claude-3-opus-20240229",
        },
        "gemini": {"gemini-flash": "gemini-1.5-flash", "gemini-pro": "gemini-1.5-pro"},
    }

    def __init__(self):
        self._clients: Dict[str, OpenAI] = {}
        self._cache: Dict[str, LLMCacheEntry] = {}
        self._usage_tracker: Dict[str, int] = {}  # dataset_id -> call count

        # Initialize provider clients
        self._setup_clients()

    def _setup_clients(self):
        """Initialize provider-specific OpenAI clients."""
        provider = settings.llm_provider or "openai"

        # OpenAI (native). Create even if api_key not set and rely on env var fallback.
        try:
            if hasattr(settings, "openai_api_key") and settings.openai_api_key:
                self._clients["openai"] = OpenAI(
                    api_key=settings.openai_api_key,
                    timeout=(
                        settings.llm_timeout if hasattr(settings, "llm_timeout") else 30
                    ),
                )
            else:
                # Will pick up OPENAI_API_KEY from environment if present
                self._clients["openai"] = OpenAI(
                    timeout=(
                        settings.llm_timeout if hasattr(settings, "llm_timeout") else 30
                    )
                )
        except Exception:
            # If client cannot be created, leave unconfigured; executors will fallback
            pass

        # Anthropic via OpenAI SDK
        if hasattr(settings, "anthropic_api_key") and settings.anthropic_api_key:
            self._clients["anthropic"] = OpenAI(
                api_key=settings.anthropic_api_key,
                base_url=self.ALLOWED_BASE_URLS["anthropic"],
                timeout=(
                    settings.llm_timeout if hasattr(settings, "llm_timeout") else 30
                ),
            )

        # Google Gemini via OpenAI SDK (if supported)
        if hasattr(settings, "google_api_key") and settings.google_api_key:
            self._clients["gemini"] = OpenAI(
                api_key=settings.google_api_key,
                base_url=self.ALLOWED_BASE_URLS["gemini"],
                timeout=(
                    settings.llm_timeout if hasattr(settings, "llm_timeout") else 30
                ),
            )

    def _get_provider_model(self, model: Optional[str] = None) -> Tuple[str, str]:
        """Get provider and actual model name."""
        model = model or settings.llm_model or "gpt-4.1-mini"
        provider = settings.llm_provider or "openai"

        # Map friendly names to provider-specific names
        if provider in self.MODEL_MAPPINGS:
            model = self.MODEL_MAPPINGS[provider].get(model, model)

        return provider, model

    def _generate_context_hash(self, context: Dict[str, Any]) -> str:
        """Generate a hash of the context for caching."""
        # Sort keys to ensure consistent hashing
        context_str = json.dumps(context, sort_keys=True, default=str)
        return hashlib.sha256(context_str.encode()).hexdigest()[:16]

    def _check_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Check if response is cached and still valid."""
        if cache_key not in self._cache:
            return None

        entry = self._cache[cache_key]
        cache_ttl = getattr(settings, "llm_cache_ttl", 86400)  # 24 hours default

        if time.time() - entry.timestamp > cache_ttl:
            del self._cache[cache_key]
            return None

        return entry.response

    def _cache_response(
        self,
        cache_key: str,
        response: Dict[str, Any],
        provider: str,
        model: str,
        usage: Optional[Dict[str, Any]] = None,
    ):
        """Cache an LLM response."""
        self._cache[cache_key] = LLMCacheEntry(
            response=response,
            timestamp=time.time(),
            model=model,
            provider=provider,
            usage=usage,
        )

    def _check_usage_limits(self, dataset_id: str):
        """Check if dataset has exceeded usage limits."""
        max_calls = getattr(settings, "llm_max_calls_per_dataset", 10)
        current_calls = self._usage_tracker.get(dataset_id, 0)

        if current_calls >= max_calls:
            raise LLMUsageError(
                f"Dataset {dataset_id} has exceeded maximum LLM calls ({max_calls})"
            )

    def _track_usage(self, dataset_id: str):
        """Track usage for a dataset."""
        self._usage_tracker[dataset_id] = self._usage_tracker.get(dataset_id, 0) + 1

    def _sanitize_user_input(self, user_input: str) -> str:
        """Sanitize user input to prevent injection attacks."""
        if not user_input:
            return ""

        # Limit length
        max_length = 1000
        if len(user_input) > max_length:
            user_input = user_input[:max_length] + "..."

        # Basic sanitization (remove control characters)
        sanitized = "".join(
            char for char in user_input if ord(char) >= 32 or char in "\n\t"
        )

        return sanitized

    @retry_with_backoff()
    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        request_id: Optional[str] = None,
    ) -> Tuple[str, LLMRequestMetrics]:
        """
        Send a chat completion request.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model name (optional, uses default)
            temperature: Temperature for sampling
            max_tokens: Maximum tokens to generate
            request_id: Request ID for tracking

        Returns:
            Tuple of (response_text, metrics)
        """
        start_time = time.time()
        provider, actual_model = self._get_provider_model(model)

        if provider not in self._clients:
            raise ValueError(f"Provider {provider} not configured")

        # Sanitize user messages
        sanitized_messages = []
        for msg in messages:
            sanitized_msg = msg.copy()
            if msg.get("role") == "user":
                sanitized_msg["content"] = self._sanitize_user_input(msg["content"])
            sanitized_messages.append(sanitized_msg)

        try:
            client = self._clients[provider]
            response = await asyncio.to_thread(
                client.chat.completions.create,
                model=actual_model,
                messages=sanitized_messages,
                temperature=temperature or settings.llm_temperature,
                max_tokens=max_tokens or settings.llm_max_tokens,
            )

            latency_ms = int((time.time() - start_time) * 1000)
            content = response.choices[0].message.content
            usage = response.usage.model_dump() if response.usage else None

            metrics = LLMRequestMetrics(
                request_id=request_id,
                provider=provider,
                model=actual_model,
                latency_ms=latency_ms,
                usage=usage,
            )

            return content, metrics

        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            metrics = LLMRequestMetrics(
                request_id=request_id,
                provider=provider,
                model=actual_model,
                latency_ms=latency_ms,
                error=str(e),
            )
            raise e

    async def chat_json(
        self,
        messages: List[Dict[str, str]],
        response_model: Optional[type] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        request_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        function_name: str = "unknown",
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], LLMRequestMetrics]:
        """
        Send a JSON-formatted chat completion request with caching and validation.

        Args:
            messages: List of message dicts
            response_model: Pydantic model for validation
            model: Model name
            temperature: Temperature for sampling
            max_tokens: Maximum tokens
            request_id: Request ID for tracking
            dataset_id: Dataset ID for usage tracking
            function_name: Function name for caching
            context: Context dict for cache key generation

        Returns:
            Tuple of (parsed_json, metrics)
        """
        # Check usage limits if dataset_id provided
        if dataset_id:
            self._check_usage_limits(dataset_id)

        # Generate cache key
        cache_key = None
        if context:
            context_hash = self._generate_context_hash(context)
            provider, actual_model = self._get_provider_model(model)
            cache_key = f"{function_name}:{actual_model}:{context_hash}"

            # Check cache first
            cached_response = self._check_cache(cache_key)
            if cached_response:
                metrics = LLMRequestMetrics(
                    request_id=request_id,
                    provider=provider,
                    model=actual_model,
                    latency_ms=0,
                    cached=True,
                )
                return cached_response, metrics

        start_time = time.time()
        provider, actual_model = self._get_provider_model(model)

        # Add JSON format instruction to system message if not present
        system_msg_found = False
        for msg in messages:
            if msg.get("role") == "system":
                system_msg_found = True
                if "JSON" not in msg["content"]:
                    msg[
                        "content"
                    ] += "\n\nIMPORTANT: Output MUST be valid JSON only. No prose outside JSON."
                break

        if not system_msg_found:
            messages.insert(
                0,
                {
                    "role": "system",
                    "content": "Output MUST be valid JSON only. No prose outside JSON.",
                },
            )

        try:
            # Get raw text response
            response_text, base_metrics = await self.chat(
                messages=messages,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                request_id=request_id,
            )

            # Parse JSON
            try:
                response_json = json.loads(response_text.strip())
            except json.JSONDecodeError as e:
                # Try to extract JSON from response
                import re

                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                if json_match:
                    try:
                        response_json = json.loads(json_match.group())
                    except json.JSONDecodeError:
                        raise LLMValidationError(f"Invalid JSON response: {str(e)}")
                else:
                    raise LLMValidationError(
                        f"No JSON found in response: {response_text[:200]}"
                    )

            # Validate with Pydantic model if provided
            if response_model:
                try:
                    validated = response_model(**response_json)
                    response_json = validated.model_dump()
                except ValidationError as e:
                    raise LLMValidationError(f"Response validation failed: {str(e)}")

            # Cache the response
            if cache_key:
                self._cache_response(
                    cache_key, response_json, provider, actual_model, base_metrics.usage
                )

            # Track usage
            if dataset_id:
                self._track_usage(dataset_id)

            return response_json, base_metrics

        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            metrics = LLMRequestMetrics(
                request_id=request_id,
                provider=provider,
                model=actual_model,
                latency_ms=latency_ms,
                error=str(e),
            )
            raise e

    def get_usage_stats(self, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """Get usage statistics."""
        if dataset_id:
            return {
                "dataset_id": dataset_id,
                "calls_made": self._usage_tracker.get(dataset_id, 0),
                "max_calls": getattr(settings, "llm_max_calls_per_dataset", 10),
            }

        return {
            "total_datasets": len(self._usage_tracker),
            "total_calls": sum(self._usage_tracker.values()),
            "cache_entries": len(self._cache),
            "per_dataset": dict(self._usage_tracker),
        }

    def clear_cache(self):
        """Clear the response cache."""
        self._cache.clear()

    def reset_usage(self, dataset_id: Optional[str] = None):
        """Reset usage tracking."""
        if dataset_id:
            self._usage_tracker.pop(dataset_id, None)
        else:
            self._usage_tracker.clear()


# Singleton instance
llm_client = LLMClient()
