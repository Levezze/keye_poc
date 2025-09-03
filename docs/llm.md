# LLM Integration Documentation

The Keye POC includes a robust, provider-agnostic LLM layer that enhances deterministic analysis with valuable insights while maintaining full auditability and security.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    LLM Layer Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  API Routes (Optional)                                      │
│  ├─ GET /api/v1/insights/{dataset_id}                      │
│  └─ Enhanced with LLM results                              │
├─────────────────────────────────────────────────────────────┤
│  Core Components                                            │
│  ├─ executors.py      (High-level coordination)            │
│  ├─ prompt_builders.py (Secure prompt construction)        │
│  ├─ types.py          (Pydantic contracts)                 │
│  └─ llm_client.py     (Provider-agnostic client)           │
├─────────────────────────────────────────────────────────────┤
│  Providers (via OpenAI SDK)                                │
│  ├─ OpenAI           (native)                              │
│  ├─ Anthropic Claude (base_url override)                   │
│  └─ Google Gemini    (base_url override)                   │
├─────────────────────────────────────────────────────────────┤
│  Storage & Audit                                           │
│  ├─ registry.py       (Artifact persistence)               │
│  └─ storage/datasets/{id}/llm/ (Audit trail)               │
└─────────────────────────────────────────────────────────────┘
```

## Core Principles

### 1. Advisory Only
- LLM **never** computes metrics or performs calculations
- All insights based strictly on deterministic analysis results
- Fallback responses when LLM unavailable

### 2. Full Auditability
- Every prompt and response persisted with metadata
- Context hash for deduplication and caching
- Request ID tracking for full traceability

### 3. Security First
- PII redaction in all contexts
- Injection-safe prompt construction
- Base URL allowlist for providers
- No secrets in artifacts

### 4. Cost & Reliability Controls
- Per-dataset usage limits
- Comprehensive caching with TTL
- Retry logic with exponential backoff
- Graceful degradation

## LLM Functions

### 1. Schema Description
Enhances dataset schemas with business context and quality insights.

**Input:** Schema metadata, column statistics
**Output:** Column descriptions, business context, quality notes, recommended analyses

### 2. Narrative Insights
Generates executive summaries from concentration analysis results.

**Input:** Concentration metrics, schema context, thresholds
**Output:** Executive summary, key findings, risk indicators, opportunities, recommendations

### 3. Risk Assessment
Evaluates concentration risk levels based on provided metrics.

**Input:** Concentration percentages and entity counts
**Output:** Risk level (low/medium/high), specific reasons, optional score

### 4. Data Quality Report
Assesses data quality from schema warnings and statistics.

**Input:** Schema metadata, normalization warnings, null rates
**Output:** Quality issues, remediation recommendations, severity score

### 5. Threshold Recommendations
Suggests optimal concentration analysis thresholds.

**Input:** Current concentration distribution, existing thresholds
**Output:** Recommended thresholds, rationale

### 6. Q&A Over Context
Answers user questions based on available analysis data.

**Input:** User question, analysis context (schema + results)
**Output:** Answer, citations, confidence level

## Configuration

### Environment Variables

```bash
# Provider Selection
LLM_PROVIDER=openai          # openai | anthropic | gemini
LLM_MODEL=gpt-4.1-mini      # Model name (friendly mapping applied)

# API Keys
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_API_KEY=AIza...

# Feature Toggle
USE_LLM=true                # Enable/disable LLM features
```

### Settings Configuration

```python
# config/settings.py
class Settings(BaseSettings):
    # Core LLM settings
    use_llm: bool = True
    llm_provider: str = "openai"
    llm_model: str = "gpt-4.1-mini"
    llm_temperature: float = 0.3
    llm_max_tokens: int = 2000
    
    # Reliability & Performance
    llm_timeout: int = 30
    llm_max_retries: int = 2
    llm_cache_ttl: int = 86400  # 24 hours
    llm_max_calls_per_dataset: int = 10
    
    # API Keys
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    google_api_key: Optional[str] = None
```

## Provider Configuration

### OpenAI (Native)
```python
# Uses standard OpenAI SDK configuration
client = OpenAI(api_key=settings.openai_api_key)
```

### Anthropic Claude
```python
# Uses OpenAI SDK with base_url override
client = OpenAI(
    api_key=settings.anthropic_api_key,
    base_url="https://api.anthropic.com/v1/"
)
```

### Google Gemini
```python
# Uses OpenAI SDK with base_url override
client = OpenAI(
    api_key=settings.google_api_key,
    base_url="https://generativelanguage.googleapis.com/v1beta/"
)
```

## Usage Examples

### Basic Usage
```python
from core.llm.executors import llm_executor

# Generate schema description
description, status = await llm_executor.generate_schema_description(
    dataset_id="ds_abc123",
    schema=schema_dict,
    dataset_stats={"row_count": 1000},
    request_id="req_xyz789"
)

# Generate insights
insights, status = await llm_executor.generate_narrative_insights(
    dataset_id="ds_abc123",
    concentration_results=analysis_results,
    schema=schema_dict,
    thresholds=[10, 20, 50],
    request_id="req_xyz789"
)
```

### Comprehensive Insights
```python
# Generate all insights at once
full_insights = await llm_executor.generate_full_insights(
    dataset_id="ds_abc123",
    concentration_results=analysis_results,
    schema=schema_dict,
    thresholds=[10, 20, 50],
    request_id="req_xyz789"
)

# Result includes:
# - narrative_insights
# - risk_assessment  
# - threshold_recommendations
# - overall_llm_status
```

### Synchronous Usage
```python
from core.llm.schema_describer import SchemaDescriber

describer = SchemaDescriber()
enhanced_schema = describer.enhance_schema(
    base_schema=schema_dict,
    dataset_id="ds_abc123",
    request_id="req_xyz789"
)
```

## Response Format

All LLM functions return structured responses with status information:

```python
@dataclass
class LLMResult:
    data: Dict[str, Any]      # Actual LLM response
    llm_status: LLMStatus     # Execution metadata
    
class LLMStatus:
    used: bool                # Whether LLM was actually invoked
    reason: Optional[str]     # Reason if not used (disabled, cached, error)
    model: Optional[str]      # Model used
    latency_ms: Optional[int] # Request latency
    cached: bool             # Whether served from cache
```

## Caching Behavior

The system implements intelligent caching to optimize performance and costs:

### Cache Key Generation
```python
cache_key = f"{function_name}:{model}:{context_hash}"
```

### Cache TTL
- Default: 24 hours
- Configurable via `llm_cache_ttl` setting
- Automatic cleanup of expired entries

### Cache Benefits
- Reduces API costs for repeated requests
- Improves response times
- Provides consistency during development

## Cost Controls

### Usage Tracking
- Per-dataset call limits (default: 10 calls)
- Global usage statistics
- Usage reset capabilities

### Budget Protection
```python
# Check usage before request
if calls_made >= max_calls:
    return fallback_response()
```

### Model Selection
- Default to smaller, cost-effective models
- Option to upgrade for complex scenarios
- Provider-specific model mappings

## Security Features

### PII Redaction
Automatically detects and redacts:
- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers
- Suspicious column patterns

### Injection Protection
- System prompts always first
- Context data always last
- User input sanitization
- Pattern detection for malicious content

### Base URL Allowlist
```python
ALLOWED_BASE_URLS = {
    "openai": "https://api.openai.com/v1/",
    "anthropic": "https://api.anthropic.com/v1/",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/"
}
```

## Audit Trail

### Artifact Storage
```
storage/datasets/{dataset_id}/llm/
├── schema_description_1699123456.json
├── narrative_insights_1699123567.json
└── risk_flags_1699123678.json
```

### Artifact Content
```json
{
  "function_name": "narrative_insights",
  "request_id": "req_abc123",
  "dataset_id": "ds_xyz789",
  "timestamp": "2023-11-04T10:30:00Z",
  "model": "gpt-4.1-mini",
  "provider": "openai",
  "context_hash": "a1b2c3d4",
  "response": {...},
  "latency_ms": 1250,
  "usage": {
    "prompt_tokens": 500,
    "completion_tokens": 200
  },
  "cached": false,
  "error": null
}
```

## Demo Script

Run comprehensive demonstrations:

```bash
# Full demo
python scripts/llm_demo.py

# Schema export only
python scripts/llm_demo.py --schemas-only

# Help
python scripts/llm_demo.py --help
```

## Error Handling

### Fallback Strategy
1. **LLM Disabled**: Return structured fallback responses
2. **Usage Limits**: Conservative responses with limit warnings
3. **API Errors**: Retry with exponential backoff, then fallback
4. **Validation Errors**: Log for debugging, return fallback
5. **Timeout**: Cancel request, return fallback

### Error Categories
- `disabled`: LLM feature turned off
- `usage_limit`: Dataset exceeded call quota
- `validation_error`: Response failed schema validation
- `api_error`: Provider API failure
- `timeout`: Request exceeded time limit

## Performance Considerations

### Async by Default
All LLM operations are async for optimal performance:
```python
# Parallel execution
tasks = [
    generate_insights(...),
    generate_risk_flags(...),
    generate_recommendations(...)
]
results = await asyncio.gather(*tasks)
```

### Batching
Use `generate_full_insights()` for multiple related functions to optimize API calls and improve consistency.

### Monitoring
- Request latency tracking
- Usage statistics
- Cache hit rates
- Error rates by provider

## Troubleshooting

### Common Issues

**"LLM disabled" responses**
- Check `USE_LLM=true` in environment
- Verify API keys are set
- Confirm provider configuration

**High latency**
- Check model selection (use mini models for faster responses)
- Verify network connectivity to provider
- Review cache hit rates

**Usage limit exceeded**
- Increase `llm_max_calls_per_dataset`
- Reset usage counters for testing
- Check cache for duplicate requests

**Validation errors**
- Review prompt outputs in artifacts
- Check model compatibility
- Verify JSON schema definitions

### Debug Information

Enable debug logging:
```python
import logging
logging.getLogger('core.llm').setLevel(logging.DEBUG)
```

Check artifacts:
```bash
ls storage/datasets/*/llm/
cat storage/datasets/{dataset_id}/llm/{artifact}.json
```

## Future Enhancements

### Planned Features
- Structured outputs with JSON schema enforcement
- Multi-turn conversations
- Custom prompt templates
- A/B testing framework
- Advanced caching strategies

### Extension Points
- Custom LLM functions
- Additional providers
- Specialized prompts by domain
- Integration with vector databases