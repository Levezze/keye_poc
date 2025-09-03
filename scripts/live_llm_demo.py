#!/usr/bin/env python3
"""
Live LLM Demo (Print-only)

- Uses real .env keys via config/settings
- Requires a valid dataset_id with deterministic artifacts already present
- Calls LLM functions and prints results nicely, but does NOT write artifacts
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


def load_context(dataset_id: str):
    base = settings.datasets_path / dataset_id
    schema_path = base / "schema.json"
    analysis_path = base / "analyses" / "concentration.json"
    if not schema_path.exists() or not analysis_path.exists():
        raise FileNotFoundError(
            "Missing schema.json or analyses/concentration.json for dataset"
        )
    schema = json.loads(schema_path.read_text())
    analysis = json.loads(analysis_path.read_text())
    thresholds = analysis.get("thresholds") or [10, 20, 50]
    return schema, analysis, thresholds


async def run_demo(dataset_id: str, provider: str | None, model: str | None):
    print("Live LLM Demo (print-only)")
    print(f"Dataset: {dataset_id}")
    print(
        f"Provider: {provider or settings.llm_provider} | Model: {model or settings.llm_model}"
    )
    print("-" * 60)

    schema, analysis, thresholds = load_context(dataset_id)

    # Narrative
    insights, s_ins = await llm_executor.generate_narrative_insights(
        dataset_id, analysis, schema, thresholds, request_id="live-demo", model=model
    )
    print("\nNarrative Insights:")
    print(json.dumps(insights.model_dump(), indent=2)[:800])
    print(f"LLM used: {s_ins.used}, reason: {s_ins.reason or ''}")

    # Risk
    risk, s_risk = await llm_executor.generate_risk_flags(
        dataset_id, analysis, request_id="live-demo", model=model
    )
    print("\nRisk Flags:")
    print(json.dumps(risk.model_dump(), indent=2))
    print(f"LLM used: {s_risk.used}, reason: {s_risk.reason or ''}")

    # Thresholds
    thr, s_thr = await llm_executor.generate_threshold_recommendations(
        dataset_id, analysis, thresholds, request_id="live-demo", model=model
    )
    print("\nThreshold Recommendations:")
    print(json.dumps(thr.model_dump(), indent=2))
    print(f"LLM used: {s_thr.used}, reason: {s_thr.reason or ''}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Live LLM demo (print-only)")
    parser.add_argument("--dataset-id", required=True, help="Dataset ID to use")
    parser.add_argument("--provider", default=None, help="Override provider")
    parser.add_argument("--model", default=None, help="Override model")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_demo(args.dataset_id, args.provider, args.model)))


if __name__ == "__main__":
    main()
