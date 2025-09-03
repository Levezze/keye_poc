# ADR-007: LLM Integration Architecture

## Status
Accepted

## Context
The Keye POC requires LLM integration to provide AI-enhanced business intelligence on top of deterministic concentration analysis. The system needed to support both automated LLM execution and manual control for error recovery and testing scenarios.

## Decision
We implemented a dual-mode LLM integration architecture:

### 1. Automatic LLM Execution
- **Trigger**: Background task after concentration analysis completes
- **Default**: Enabled (`run_llm: true` in ConcentrationRequest)
- **Functions**: narrative_insights, risk_flags, threshold_recommendations
- **Storage**: Timestamped artifacts in `storage/datasets/{dataset_id}/llm/`
- **Fallback**: Graceful degradation when LLM providers unavailable

### 2. Manual LLM Control
- **Endpoint**: `POST /api/v1/analyze/{dataset_id}/llm`
- **Use Cases**: Re-running after API key fixes, regenerating poor results, testing different functions
- **Parameters**: `force_refresh`, `functions` selection
- **Response**: Execution summary with artifacts created and LLM status

### 3. Provider Fallback Chain
- **Primary**: OpenAI (gpt-4.1-mini)
- **Fallback**: Gemini → Claude Haiku
- **Offline Mode**: Mock data artifacts for testing without API keys
- **Error Handling**: Clean fallback responses when providers unavailable

### 4. Artifact Management
- **Format**: Timestamped JSON files (e.g., `narrative_insights_1756883732.json`)
- **Location**: `storage/datasets/{dataset_id}/llm/`
- **Versioning**: Multiple artifacts supported, latest used by default
- **Lineage**: Full audit trail of LLM executions in dataset lineage

## Consequences

### Positive
- **Automatic Enhancement**: Users get AI insights without additional steps
- **Error Recovery**: Manual re-execution when API keys fail or results are poor
- **Flexibility**: Can run specific LLM functions or skip LLM entirely
- **Offline Testing**: Mock data allows testing without API dependencies
- **Auditability**: Complete lineage tracking of LLM executions

### Negative
- **Complexity**: Two execution paths (automatic + manual) require coordination
- **Storage**: Multiple LLM artifacts per dataset increase storage usage
- **Latency**: Background tasks delay LLM availability (but don't block concentration results)

## Implementation Details

### Background Task Architecture
```python
# Triggered after concentration analysis
if request.run_llm:
    background_tasks.add_task(
        _run_llm_analysis_background,
        dataset_id, concentration_results, schema, thresholds, registry
    )
```

### Manual Endpoint
```python
POST /api/v1/analyze/{dataset_id}/llm
{
  "force_refresh": true,
  "functions": ["narrative_insights", "risk_flags"]
}
```

### Artifact Structure
- Location: `storage/datasets/{dataset_id}/llm/`
- Files: `{function_name}_{timestamp}.json`
- Content: LLM response + metadata (provider, model, execution time)

### Integration Points
- **Concentration Analysis**: Automatic LLM trigger via BackgroundTasks
- **Insights Endpoint**: Uses LLM artifacts when available, falls back to deterministic
- **Lineage System**: Tracks LLM executions with parameters and results

## Alternatives Considered

### Synchronous LLM Execution
**Rejected**: Would add 5-10 seconds to concentration analysis response time

### Single LLM Endpoint Only
**Rejected**: No automatic enhancement, users would need manual steps

### In-Memory LLM Results
**Rejected**: Loss of auditability and inability to recover from failures

## Related Decisions
- ADR-001: Storage Architecture (provides foundation for LLM artifacts)
- ADR-003: API Design Patterns (background task pattern used here)

## Notes
- Mock data directory (`storage/datasets/mock_data/`) provides offline testing capability
- LLM functions are advisory only - never compute business metrics directly
- All calculations remain deterministic with LLM providing narrative enhancement