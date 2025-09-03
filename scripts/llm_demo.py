#!/usr/bin/env python3
"""
LLM Demo Script

Comprehensive demonstration of all LLM functions with real dataset testing.
Shows provider switching, caching, usage tracking, and audit trails.
"""

import asyncio
import json
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
import uuid

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from services.registry import DatasetRegistry
from services.llm_client import llm_client
from core.llm.executors import llm_executor
from core.llm.types import export_schemas


def load_sample_data() -> Optional[Dict[str, Any]]:
    """Load sample concentration analysis results for testing."""
    # Sample data that mimics real concentration analysis output
    return {
        "by_period": [
            {
                "period": "2023-Q1",
                "total": 2500000.0,
                "total_entities": 500,
                "concentration": {
                    "top_10": {"count": 50, "value": 2000000.0, "percentage": 80.0},
                    "top_20": {"count": 100, "value": 2250000.0, "percentage": 90.0},
                    "top_50": {"count": 250, "value": 2450000.0, "percentage": 98.0},
                },
                "head": [
                    {"entity": "Customer_001", "value": 300000.0},
                    {"entity": "Customer_002", "value": 250000.0},
                    {"entity": "Customer_003", "value": 200000.0},
                ],
            }
        ],
        "totals": {
            "period": "TOTAL",
            "total_entities": 500,
            "total_value": 2500000.0,
            "concentration": {
                "top_10": {"count": 50, "value": 2000000.0, "percentage": 80.0},
                "top_20": {"count": 100, "value": 2250000.0, "percentage": 90.0},
            },
        },
    }


def load_sample_schema() -> Dict[str, Any]:
    """Load sample schema for testing."""
    return {
        "dataset_id": "demo_dataset_001",
        "period_grain": "quarter",
        "columns": [
            {
                "name": "customer_id",
                "original_name": "Customer ID",
                "dtype": "object",
                "role": "categorical",
                "cardinality": 500,
                "null_rate": 0.0,
            },
            {
                "name": "transaction_amount",
                "original_name": "Transaction Amount",
                "dtype": "float64",
                "role": "numeric",
                "cardinality": 489,
                "null_rate": 0.02,
            },
            {
                "name": "transaction_date",
                "original_name": "Transaction Date",
                "dtype": "datetime64[ns]",
                "role": "datetime",
                "cardinality": 90,
                "null_rate": 0.0,
            },
        ],
        "warnings": ["Some negative values in transaction_amount"],
        "notes": ["Dataset appears to be customer transaction records"],
    }


class LLMDemoRunner:
    """Comprehensive LLM demonstration runner."""

    def __init__(self):
        self.demo_dataset_id = f"demo_{uuid.uuid4().hex[:8]}"
        self.registry = DatasetRegistry()
        self.request_id = f"demo_{uuid.uuid4().hex[:8]}"

        print(f" LLM Demo Starting")
        print(f" Demo Dataset ID: {self.demo_dataset_id}")
        print(f" Request ID: {self.request_id}")
        print(f" LLM Enabled: {settings.use_llm}")
        print(f" Provider: {settings.llm_provider}")
        print(f" Model: {settings.llm_model}")
        print("-" * 60)

    def print_section(self, title: str):
        """Print a formatted section header."""
        print(f"\n{'=' * 60}")
        print(f" {title}")
        print("=" * 60)

    def print_result(self, function_name: str, result: Any, status: Any):
        """Print formatted results."""
        print(f"\n {function_name.replace('_', ' ').title()} Results:")
        print(f"   Status: {' Success' if status.get('used', False) else ' Fallback'}")
        if status.get("model"):
            print(f"   Model: {status['model']}")
        if status.get("latency_ms"):
            print(f"   Latency: {status['latency_ms']}ms")
        if status.get("cached"):
            print(f"   Cached: {' Yes' if status['cached'] else ' No'}")
        print(f"   Data: {json.dumps(result, indent=2)[:200]}...")

    async def demo_schema_description(self):
        """Demonstrate schema description function."""
        self.print_section("Schema Description")

        schema = load_sample_schema()
        dataset_stats = {"row_count": 5000, "column_count": 3}

        try:
            result, status = await llm_executor.generate_schema_description(
                dataset_id=self.demo_dataset_id,
                schema=schema,
                dataset_stats=dataset_stats,
                request_id=self.request_id,
            )

            self.print_result(
                "schema_description", result.model_dump(), status.model_dump()
            )
            return True

        except Exception as e:
            print(f" Schema description failed: {e}")
            return False

    async def demo_narrative_insights(self):
        """Demonstrate narrative insights function."""
        self.print_section("Narrative Insights")

        concentration_results = load_sample_data()
        schema = load_sample_schema()
        thresholds = [10, 20, 50]

        try:
            result, status = await llm_executor.generate_narrative_insights(
                dataset_id=self.demo_dataset_id,
                concentration_results=concentration_results,
                schema=schema,
                thresholds=thresholds,
                request_id=self.request_id,
            )

            self.print_result(
                "narrative_insights", result.model_dump(), status.model_dump()
            )
            return True

        except Exception as e:
            print(f" Narrative insights failed: {e}")
            return False

    async def demo_risk_flags(self):
        """Demonstrate risk flags function."""
        self.print_section("Risk Assessment")

        concentration_results = load_sample_data()

        try:
            result, status = await llm_executor.generate_risk_flags(
                dataset_id=self.demo_dataset_id,
                concentration_results=concentration_results,
                request_id=self.request_id,
            )

            self.print_result("risk_flags", result.model_dump(), status.model_dump())
            return True

        except Exception as e:
            print(f" Risk flags failed: {e}")
            return False

    async def demo_data_quality_report(self):
        """Demonstrate data quality report function."""
        self.print_section("Data Quality Report")

        schema = load_sample_schema()
        warnings = [
            "Some negative values detected",
            "High null rate in optional fields",
        ]

        try:
            result, status = await llm_executor.generate_data_quality_report(
                dataset_id=self.demo_dataset_id,
                schema=schema,
                normalization_warnings=warnings,
                request_id=self.request_id,
            )

            self.print_result(
                "data_quality_report", result.model_dump(), status.model_dump()
            )
            return True

        except Exception as e:
            print(f" Data quality report failed: {e}")
            return False

    async def demo_threshold_recommendations(self):
        """Demonstrate threshold recommendations function."""
        self.print_section("Threshold Recommendations")

        concentration_results = load_sample_data()
        current_thresholds = [10, 20, 50]

        try:
            result, status = await llm_executor.generate_threshold_recommendations(
                dataset_id=self.demo_dataset_id,
                concentration_results=concentration_results,
                current_thresholds=current_thresholds,
                request_id=self.request_id,
            )

            self.print_result(
                "threshold_recommendations", result.model_dump(), status.model_dump()
            )
            return True

        except Exception as e:
            print(f" Threshold recommendations failed: {e}")
            return False

    async def demo_qa_over_context(self):
        """Demonstrate Q&A over context function."""
        self.print_section("Q&A Over Context")

        context = {"analysis": load_sample_data(), "schema": load_sample_schema()}

        questions = [
            "What percentage of revenue comes from the top 10% of customers?",
            "How many customers are in the dataset?",
            "What is the concentration risk level?",
        ]

        success_count = 0
        for question in questions:
            print(f"\n❓ Question: {question}")
            try:
                result, status = await llm_executor.answer_question(
                    dataset_id=self.demo_dataset_id,
                    user_question=question,
                    context=context,
                    request_id=self.request_id,
                )

                print(f"   Answer: {result.answer}")
                print(f"   Status: {'✅ Success' if status.used else '❌ Fallback'}")
                if result.citations:
                    print(f"   Citations: {result.citations}")

                success_count += 1

            except Exception as e:
                print(f" Q&A failed for question: {e}")

        return success_count > 0

    async def demo_full_insights(self):
        """Demonstrate full insights generation."""
        self.print_section("Full Insights Generation")

        concentration_results = load_sample_data()
        schema = load_sample_schema()
        thresholds = [10, 20, 50]

        try:
            result = await llm_executor.generate_full_insights(
                dataset_id=self.demo_dataset_id,
                concentration_results=concentration_results,
                schema=schema,
                thresholds=thresholds,
                request_id=self.request_id,
            )

            print(f"📊 Full Insights Generated:")
            print(f"   Overall Status: {result['overall_llm_status']}")
            print(f"   Generated At: {result['generated_at']}")

            for insight_type, insight_data in result.items():
                if isinstance(insight_data, dict) and "llm_status" in insight_data:
                    status = insight_data["llm_status"]
                    print(f"   {insight_type}: {'✅' if status.get('used') else '❌'}")

            return True

        except Exception as e:
            print(f" Full insights failed: {e}")
            return False

    def demo_usage_tracking(self):
        """Demonstrate usage tracking and caching."""
        self.print_section("Usage Tracking & Caching")

        # Get usage stats
        stats = llm_client.get_usage_stats(self.demo_dataset_id)
        print(f" Usage Statistics:")
        print(f"   Dataset: {stats.get('dataset_id', 'N/A')}")
        print(f"   Calls Made: {stats.get('calls_made', 0)}")
        print(f"   Max Calls: {stats.get('max_calls', 0)}")

        # Get overall stats
        overall_stats = llm_client.get_usage_stats()
        print(f" Overall Statistics:")
        print(f"   Total Datasets: {overall_stats.get('total_datasets', 0)}")
        print(f"   Total Calls: {overall_stats.get('total_calls', 0)}")
        print(f"   Cache Entries: {overall_stats.get('cache_entries', 0)}")

    def demo_audit_trail(self):
        """Demonstrate audit trail and artifacts."""
        self.print_section("Audit Trail")

        try:
            # Check if dataset exists in registry
            lineage = self.registry.get_lineage(self.demo_dataset_id)
            if lineage:
                print(f"📝 Lineage found for dataset {self.demo_dataset_id}")
                llm_steps = [
                    step for step in lineage.get("steps", []) if step.get("llm")
                ]
                print(f"   LLM Steps: {len(llm_steps)}")
            else:
                print(f" No lineage found for dataset {self.demo_dataset_id}")

            # List LLM artifacts
            dataset_path = settings.datasets_path / self.demo_dataset_id
            llm_path = dataset_path / "llm"

            if llm_path.exists():
                artifacts = list(llm_path.glob("*.json"))
                print(f" LLM Artifacts: {len(artifacts)}")
                for artifact in artifacts[:3]:  # Show first 3
                    print(f"   - {artifact.name}")
            else:
                print(" No LLM artifacts found")

        except Exception as e:
            print(f" Audit trail demo failed: {e}")

    def demo_json_schemas(self):
        """Demonstrate JSON schema export."""
        self.print_section("JSON Schema Export")

        try:
            schemas = export_schemas()
            print(f"📋 Available LLM Function Schemas:")
            for name, schema in schemas.items():
                print(f"   - {name}: {len(schema.get('properties', {}))} properties")

            # Show example schema
            if schemas:
                example_name = list(schemas.keys())[0]
                example_schema = schemas[example_name]
                print(f"\n📝 Example Schema ({example_name}):")
                print(json.dumps(example_schema, indent=2)[:300] + "...")

        except Exception as e:
            print(f"❌ Schema export failed: {e}")

    async def run_comprehensive_demo(self):
        """Run the complete demonstration."""
        print("🎯 Starting Comprehensive LLM Demonstration")

        # Create demo dataset for tracking
        try:
            self.registry.create_dataset(f"llm_demo_{self.request_id}.json")
        except Exception as e:
            print(f"⚠️  Could not create demo dataset: {e}")

        # Run all demos
        demos = [
            ("Schema Description", self.demo_schema_description),
            ("Narrative Insights", self.demo_narrative_insights),
            ("Risk Assessment", self.demo_risk_flags),
            ("Data Quality Report", self.demo_data_quality_report),
            ("Threshold Recommendations", self.demo_threshold_recommendations),
            ("Q&A Over Context", self.demo_qa_over_context),
            ("Full Insights", self.demo_full_insights),
        ]

        results = {}
        for name, demo_func in demos:
            try:
                success = await demo_func()
                results[name] = success
            except Exception as e:
                print(f"❌ {name} demo failed completely: {e}")
                results[name] = False

        # Run sync demos
        self.demo_usage_tracking()
        self.demo_audit_trail()
        self.demo_json_schemas()

        # Final summary
        self.print_section("Demo Summary")
        successful = sum(results.values())
        total = len(results)

        print(f"📊 Demo Results: {successful}/{total} successful")
        for name, success in results.items():
            status = "✅" if success else "❌"
            print(f"   {status} {name}")

        if successful == total:
            print("\n🎉 All demos completed successfully!")
        elif successful > 0:
            print(f"\n⚠️  {total - successful} demos failed - check LLM configuration")
        else:
            print("\n❌ All demos failed - LLM may be disabled or misconfigured")

        print(f"\n💡 Tips:")
        print(f"   - Set environment variables for your LLM provider")
        print(f"   - Check USE_LLM=true in .env")
        print(f"   - Verify API keys are valid")
        print(f"   - Review logs for detailed error information")


def main():
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("LLM Demo Script")
        print("Usage: python scripts/llm_demo.py [--help] [--schemas-only]")
        print("")
        print("Options:")
        print("  --help        Show this help message")
        print("  --schemas-only Export JSON schemas only")
        sys.exit(0)

    if len(sys.argv) > 1 and sys.argv[1] == "--schemas-only":
        runner = LLMDemoRunner()
        runner.demo_json_schemas()
        sys.exit(0)

    # Run full demo
    runner = LLMDemoRunner()

    try:
        asyncio.run(runner.run_comprehensive_demo())
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
