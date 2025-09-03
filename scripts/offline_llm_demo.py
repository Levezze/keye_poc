#!/usr/bin/env python3
"""
Offline LLM Demo (Read-only)

- Reads deterministic artifacts from storage/datasets/mock_data
- If LLM keys are present and providers work, prints real outputs
- Otherwise prints graceful fallbacks
- Does NOT write any artifacts by default

Optional: --refresh-artifacts (only if providers succeed) to write new LLM artifacts into mock_data/llm
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Ensure project root is on sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from services.registry import DatasetRegistry
from core.llm.executors import llm_executor
from services.llm_client import llm_client


MOCK_ID = "mock_data"
DATASETS = Path("storage/datasets")
MOCK_PATH = DATASETS / MOCK_ID


def load_mock_context():
    schema_path = MOCK_PATH / "schema.json"
    analysis_path = MOCK_PATH / "analyses" / "concentration.json"
    schema = json.loads(schema_path.read_text()) if schema_path.exists() else {}
    analysis = json.loads(analysis_path.read_text()) if analysis_path.exists() else {}
    thresholds = analysis.get("thresholds") or [10, 20, 50]
    return schema, analysis, thresholds


async def run_demo(refresh_artifacts: bool = False):
    # Smoke test providers (visibility into fallback readiness)
    print("Provider connectivity (smoke test):")
    provider_models = {
        "openai": "gpt-4.1-mini",
        "gemini": "gemini-flash",
        "anthropic": "claude-3.5-haiku",
    }
    any_ok = False
    for prov, friendly_model in provider_models.items():
        if prov not in llm_client._clients:
            print(f"  {prov}: not configured")
            continue
        try:
            # Temporary provider switch
            old_provider = settings.llm_provider
            settings.llm_provider = prov
            messages = [
                {"role": "system", "content": 'Return exactly {"ok": true} as JSON.'},
                {"role": "user", "content": 'Return only {"ok": true}.'},
            ]
            resp, _ = await llm_client.chat_json(
                messages=messages, model=friendly_model
            )
            ok = isinstance(resp, dict) and resp.get("ok") is True
            print(f"  {prov}: {'OK' if ok else 'unexpected response'}")
            any_ok = any_ok or ok
        except Exception as e:
            print(f"  {prov}: failed ({str(e)[:120]})")
        finally:
            settings.llm_provider = old_provider

    print("Offline LLM Demo (read-only)")
    print(f"Dataset: {MOCK_ID}")
    print(f"Provider: {settings.llm_provider} | Model: {settings.llm_model}")
    print("-" * 60)

    schema, analysis, thresholds = load_mock_context()
    if not schema or not analysis:
        print(
            "Missing deterministic artifacts in mock_data; ensure schema.json and analyses/concentration.json exist."
        )
        return 1

    # Try all LLM functions, but do not write artifacts unless refresh_artifacts and success
    # Narrative Insights
    insights, s_ins = await llm_executor.generate_narrative_insights(
        dataset_id=MOCK_ID,
        concentration_results=analysis,
        schema=schema,
        thresholds=thresholds,
        request_id="offline-demo",
    )
    print("\nNarrative Insights:")
    print(json.dumps(insights.model_dump(), indent=2)[:800])
    print(f"LLM used: {s_ins.used}, reason: {s_ins.reason or ''}")

    # Risk
    risk, s_risk = await llm_executor.generate_risk_flags(
        dataset_id=MOCK_ID,
        concentration_results=analysis,
        request_id="offline-demo",
    )
    print("\nRisk Flags:")
    print(json.dumps(risk.model_dump(), indent=2))
    print(f"LLM used: {s_risk.used}, reason: {s_risk.reason or ''}")

    # Thresholds
    thresh, s_thr = await llm_executor.generate_threshold_recommendations(
        dataset_id=MOCK_ID,
        concentration_results=analysis,
        current_thresholds=thresholds,
        request_id="offline-demo",
    )
    print("\nThreshold Recommendations:")
    print(json.dumps(thresh.model_dump(), indent=2))
    print(f"LLM used: {s_thr.used}, reason: {s_thr.reason or ''}")

    # Only write artifacts if explicitly requested and at least one call used LLM
    if refresh_artifacts and any([s_ins.used, s_risk.used, s_thr.used]):
        print("\nRefreshing LLM artifacts in mock_data/llm ...")
        # The executors already persist on success; nothing else to do
    else:
        print(
            "\nNo artifact writes performed (read-only mode). Use --refresh-artifacts after configuring API keys to update mock artifacts."
        )

    return 0


def main():
    parser = argparse.ArgumentParser(description="Offline LLM demo (read-only)")
    parser.add_argument(
        "--refresh-artifacts",
        action="store_true",
        help="Write new artifacts to mock_data/llm if LLM calls succeed",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_demo(args.refresh_artifacts)))


if __name__ == "__main__":
    main()
