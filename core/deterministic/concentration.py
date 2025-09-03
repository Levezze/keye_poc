"""
Concentration Analysis Module
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple, cast
from dataclasses import dataclass


@dataclass
class ConcentrationResult:
    """Result of concentration analysis."""

    data: Dict[str, Any]
    parameters: Dict[str, Any]
    computation_log: List[Dict[str, Any]]
    formulas: Dict[str, str]


class ConcentrationAnalyzer:
    """
    Performs concentration analysis on data with deterministic tie-breaking.
    
    Threshold Semantics:
    - Thresholds represent percentage cutoffs (e.g., 10 = 10%)
    - For each threshold X, includes entities whose cumulative percentage ≤ X%
    - If no entities qualify for a threshold, includes at least the top 1 entity
    - Tie-breaking: ORDER BY value DESC, then group_by ASC (deterministic)
    
    Examples:
    - Entities: A(100), B(80), C(60), Total=240
    - Cumulative %: A(41.7%), B(75%), C(100%)
    - Threshold 10%: A only (41.7% > 10% but at least 1 entity included)
    - Threshold 50%: A + B (75% > 50% but B's individual contribution fits)
    """

    def analyze(
        self,
        df: pd.DataFrame,
        group_by: str,
        value_column: str,
        period_key_column: Optional[str] = None,
        thresholds: Optional[List[int]] = None,
    ) -> ConcentrationResult:
        """
        Perform concentration analysis with deterministic tie-breaking.

        Analyzes data concentration by computing cumulative percentages and finding
        entities that contribute to each threshold percentage of total value.

        Args:
            df: Input DataFrame
            group_by: Column to group by (e.g., entity, customer)
            value_column: Column to aggregate (e.g., revenue, sales)
            period_key_column: Optional period column for time-based analysis
            thresholds: Concentration thresholds in percentage (default [10, 20, 50])
                       Each threshold X includes entities with cumulative % ≤ X%,
                       with at least 1 entity if none qualify.

        Returns:
            ConcentrationResult with analysis data including:
            - Per-threshold counts, values, and percentages
            - Head sample of top entities (for display)
            - Computation log for auditability
            - Formula documentation

        Raises:
            Will return error in results if total value ≤ 0 (cannot compute percentages)
        """
        if thresholds is None:
            thresholds = [10, 20, 50]

        parameters = {
            "group_by": group_by,
            "value_column": value_column,
            "period_key_column": period_key_column,
            "thresholds": thresholds,
            "total_rows": len(df),
            "analysis_type": "multi_period" if period_key_column else "single_period",
        }

        computation_log = []
        results = {}

        try:
            if period_key_column and period_key_column in df.columns:
                # Multi-period analysis: analyze each period separately + overall
                results, period_logs = self._analyze_multi_period(
                    df, group_by, value_column, period_key_column, thresholds
                )
                computation_log.extend(period_logs)
            else:
                # Single-period analysis
                single_result, single_log = self._analyze_single_period(
                    df, group_by, value_column, thresholds, period_name="TOTAL"
                )
                results = {"TOTAL": single_result}
                computation_log.extend(single_log)

            # Add summary statistics
            results["summary"] = self._generate_summary(results, parameters)
            computation_log.append(
                {
                    "step": "summary_generation",
                    "status": "completed",
                    "periods_analyzed": len(
                        [k for k in results.keys() if k != "summary"]
                    ),
                }
            )

        except Exception as e:
            computation_log.append(
                {"step": "analysis_error", "status": "failed", "error": str(e)}
            )
            results = {"error": str(e)}

        formulas = self._document_formulas(thresholds)

        return ConcentrationResult(
            data=results,
            parameters=parameters,
            computation_log=computation_log,
            formulas=formulas,
        )

    def _analyze_multi_period(
        self,
        df: pd.DataFrame,
        group_by: str,
        value_col: str,
        period_col: str,
        thresholds: List[int],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Analyze concentration for each period and overall."""
        results = {}
        logs = []

        # Get unique periods and sort them
        periods = sorted(df[period_col].unique())
        logs.append(
            {
                "step": "period_identification",
                "status": "completed",
                "periods_found": len(periods),
                "periods": periods[:10],  # Log first 10 for brevity
            }
        )

        # Analyze each period
        for period in periods:
            mask = df[period_col] == period
            period_df = cast(pd.DataFrame, df.loc[mask])
            if len(period_df) > 0:
                period_result, period_log = self._analyze_single_period(
                    period_df, group_by, value_col, thresholds, period_name=str(period)
                )
                results[str(period)] = period_result
                logs.extend(period_log)

        # Overall analysis (all periods combined)
        overall_result, overall_log = self._analyze_single_period(
            df, group_by, value_col, thresholds, period_name="TOTAL"
        )
        results["TOTAL"] = overall_result
        logs.extend(overall_log)

        return results, logs

    def _analyze_single_period(
        self,
        df: pd.DataFrame,
        group_by: str,
        value_col: str,
        thresholds: List[int],
        period_name: str,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Analyze concentration for a single period with deterministic tie-breaking."""
        logs = []

        # Group and aggregate
        try:
            grouped = df.groupby(group_by)[value_col].sum().reset_index()
            logs.append(
                {
                    "step": f"aggregation_{period_name}",
                    "status": "completed",
                    "entities_count": len(grouped),
                    "total_value": float(grouped[value_col].sum()),
                }
            )
        except Exception as e:
            logs.append(
                {
                    "step": f"aggregation_{period_name}",
                    "status": "failed",
                    "error": str(e),
                }
            )
            return {"error": str(e)}, logs

        # Handle edge cases
        if len(grouped) == 0:
            return {"error": "No data after grouping"}, logs

        total_value = grouped[value_col].sum()
        if total_value <= 0:
            return {"error": "Total value is non-positive; cannot compute concentration"}, logs

        # Sort with deterministic tie-breaking: value desc, then group_by asc
        grouped_sorted = grouped.sort_values(
            [value_col, group_by], ascending=[False, True]
        ).reset_index(drop=True)

        # Calculate cumulative sums and percentages
        grouped_sorted["cumsum"] = grouped_sorted[value_col].cumsum()
        grouped_sorted["cumulative_pct"] = (
            grouped_sorted["cumsum"] / total_value
        ) * 100

        # Calculate concentration thresholds
        # 
        # Threshold Logic:
        # - For threshold X%, include entities whose cumulative percentage ≤ X%
        # - This means we find the minimum set of top entities that together
        #   account for AT MOST X% of total value
        # - If no entities qualify (first entity > X%), include at least 1 entity
        # - Example: A(60%), B(30%), C(10%) with threshold 50%
        #   → A alone is 60% > 50%, but we include A (at least 1 entity rule)
        concentration = {}
        for threshold in thresholds:
            # Find entities where cumulative percentage <= threshold
            mask = grouped_sorted["cumulative_pct"] <= threshold
            entities_in_threshold = grouped_sorted[mask]

            if len(entities_in_threshold) == 0:
                # If no entities meet the threshold, include at least the first one
                # This ensures every threshold has at least one entity
                entities_in_threshold = grouped_sorted.head(1)

            concentration[f"top_{threshold}"] = {
                "count": len(entities_in_threshold),
                "value": float(entities_in_threshold[value_col].sum()),
                "percentage": float(
                    (entities_in_threshold[value_col].sum() / total_value) * 100
                ),
                "entities": entities_in_threshold[group_by].tolist()[
                    :10
                ],  # Top 10 for display
            }

        # Add head sample for display
        head_sample = grouped_sorted.head(min(20, len(grouped_sorted))).to_dict(
            "records"
        )
        for record in head_sample:
            # Convert numpy types to native Python types for JSON serialization
            for key, value in record.items():
                if isinstance(value, (np.integer, np.floating)):
                    record[key] = float(value)

        result = {
            "period": period_name,
            "total_entities": len(grouped),
            "total_value": float(total_value),
            "concentration": concentration,
            "head_sample": head_sample,
        }

        logs.append(
            {
                "step": f"concentration_calculation_{period_name}",
                "status": "completed",
                "thresholds_calculated": len(thresholds),
            }
        )

        return result, logs

    def _generate_summary(
        self, results: Dict[str, Any], parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate summary statistics across all periods."""
        summary = {
            "analysis_type": parameters["analysis_type"],
            "periods_analyzed": len([k for k in results.keys() if k != "summary"]),
            "thresholds": parameters["thresholds"],
            "total_input_rows": parameters["total_rows"],
        }

        # If multi-period, add period-level summary
        if parameters["analysis_type"] == "multi_period":
            period_summaries = []
            for period, data in results.items():
                if period not in ["summary", "TOTAL"]:
                    period_summaries.append(
                        {
                            "period": period,
                            "entities": data.get("total_entities", 0),
                            "value": data.get("total_value", 0),
                        }
                    )
            summary["periods"] = period_summaries

        return summary

    def _document_formulas(self, thresholds: List[int]) -> Dict[str, str]:
        """Document formulas used in calculations."""
        formulas = {
            "aggregation": "SUM(value_column) GROUP BY group_by_column",
            "sorting": "ORDER BY value DESC, entity ASC (deterministic tie-breaking)",
            "cumulative_percentage": "(CUMSUM(value) / TOTAL_VALUE) * 100",
        }

        for threshold in thresholds:
            formulas[f"top_{threshold}"] = (
                f"Count and sum entities where cumulative_percentage <= {threshold}%"
            )

        return formulas
