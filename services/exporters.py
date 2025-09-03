"""
Export Service
Handles exporting analysis results to various formats.
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from pathlib import Path
import json
from services.storage import StorageService


class ExportService:
    """Handles exporting analysis results."""

    @staticmethod
    def export_concentration_csv(results: Dict[str, Any], output_path: Path) -> str:
        """
        Export concentration results to CSV.

        Args:
            results: Concentration analysis results
            output_path: Output file path

        Returns:
            Path to exported file
        """
        # Build a standard single-table CSV to remain parser-friendly
        # Supports both legacy shape (top_10 keys at period root) and new shape (concentration dict)
        rows: List[Dict[str, Any]] = []

        def _append_rows_for_period(period_label: str, payload: Dict[str, Any]):
            # Preferred: nested concentration dict from analyzer
            concentration = payload.get("concentration")
            if isinstance(concentration, dict) and concentration:
                for threshold_key, metrics in concentration.items():
                    if isinstance(metrics, dict):
                        threshold_display = threshold_key.replace("top_", "")
                        try:
                            threshold_value = int(threshold_display)
                        except Exception:
                            threshold_value = threshold_display
                        rows.append(
                            {
                                "period": period_label,
                                "threshold": threshold_value,
                                "count": metrics.get("count", 0),
                                "value": metrics.get("value", 0),
                                "pct_of_total": round(
                                    metrics.get(
                                        "percentage", metrics.get("pct_of_total", 0)
                                    ),
                                    1,
                                ),
                            }
                        )
                return

            # Fallback: legacy top_* keys on the period dict
            for key, metrics in payload.items():
                if isinstance(metrics, dict) and str(key).startswith("top_"):
                    threshold_display = str(key).replace("top_", "")
                    try:
                        threshold_value = int(threshold_display)
                    except Exception:
                        threshold_value = threshold_display
                    rows.append(
                        {
                            "period": period_label,
                            "threshold": threshold_value,
                            "count": metrics.get("count", 0),
                            "value": metrics.get("value", 0),
                            "pct_of_total": round(
                                metrics.get(
                                    "pct_of_total", metrics.get("percentage", 0)
                                ),
                                1,
                            ),
                        }
                    )

        # by_period entries
        for period_data in results.get("by_period", []) or []:
            period = period_data.get("period", "TOTAL")
            _append_rows_for_period(period, period_data)

        # totals fallback for single-period datasets
        if not rows and isinstance(results.get("totals"), dict):
            _append_rows_for_period("TOTAL", results["totals"])

        df = pd.DataFrame(rows)
        return StorageService.write_csv(df, output_path)

    @staticmethod
    def export_concentration_excel(
        results: Dict[str, Any], output_path: Path, include_formulas: bool = True
    ) -> str:
        """
        Export concentration results to Excel with multiple sheets.

        Args:
            results: Concentration analysis results
            output_path: Output file path
            include_formulas: Whether to include audit formulas

        Returns:
            Path to exported file
        """
        sheets = {}

        # Summary sheet
        summary_rows: List[Dict[str, Any]] = []
        if results.get("by_period"):
            for period_data in results["by_period"]:
                row: Dict[str, Any] = {
                    "period": period_data.get("period"),
                    "total": period_data.get("total", 0),
                }
                # Preferred: nested concentration dict
                concentration = period_data.get("concentration")
                if isinstance(concentration, dict) and concentration:
                    for threshold_key, metrics in concentration.items():
                        if isinstance(metrics, dict):
                            row[f"{threshold_key}_count"] = metrics.get("count", 0)
                            row[f"{threshold_key}_value"] = metrics.get("value", 0)
                            row[f"{threshold_key}_pct"] = round(
                                metrics.get(
                                    "percentage", metrics.get("pct_of_total", 0)
                                ),
                                1,
                            )
                else:
                    # Fallback: legacy top_* keys on the period dict
                    for key, metrics in period_data.items():
                        if isinstance(metrics, dict) and str(key).startswith("top_"):
                            row[f"{key}_count"] = metrics.get("count", 0)
                            row[f"{key}_value"] = metrics.get("value", 0)
                            row[f"{key}_pct"] = round(
                                metrics.get(
                                    "pct_of_total", metrics.get("percentage", 0)
                                ),
                                1,
                            )
                summary_rows.append(row)
        elif isinstance(results.get("totals"), dict):
            # Handle single-period case (no time dimension)
            totals_data = results["totals"]
            row = {"period": "TOTAL", "total": totals_data.get("total", 0)}
            concentration = totals_data.get("concentration", {})
            if isinstance(concentration, dict) and concentration:
                for threshold_key, metrics in concentration.items():
                    if isinstance(metrics, dict):
                        row[f"{threshold_key}_count"] = metrics.get("count", 0)
                        row[f"{threshold_key}_value"] = metrics.get("value", 0)
                        row[f"{threshold_key}_pct"] = round(
                            metrics.get("percentage", metrics.get("pct_of_total", 0)), 1
                        )
            else:
                for key, metrics in totals_data.items():
                    if isinstance(metrics, dict) and str(key).startswith("top_"):
                        row[f"{key}_count"] = metrics.get("count", 0)
                        row[f"{key}_value"] = metrics.get("value", 0)
                        row[f"{key}_pct"] = round(
                            metrics.get("pct_of_total", metrics.get("percentage", 0)), 1
                        )
            summary_rows.append(row)

        if summary_rows:
            sheets["Summary"] = pd.DataFrame(summary_rows)

        # Details sheet (if available)
        if "details" in results:
            sheets["Details"] = pd.DataFrame(results["details"])

        # Head samples sheet - show top entities for each period
        head_sample_rows = []
        if "by_period" in results and len(results["by_period"]) > 0:
            for period_data in results["by_period"]:
                period = period_data["period"]
                head_sample = period_data.get("head_sample", [])
                for i, entity in enumerate(
                    head_sample[:10]
                ):  # Limit to top 10 for readability
                    row = entity.copy()
                    row["period"] = period
                    row["rank"] = i + 1
                    head_sample_rows.append(row)
        elif "totals" in results:
            # Single period case
            totals_data = results["totals"]
            head_sample = totals_data.get("head_sample", [])
            for i, entity in enumerate(head_sample[:10]):
                row = entity.copy()
                row["period"] = "TOTAL"
                row["rank"] = i + 1
                head_sample_rows.append(row)

        if head_sample_rows:
            sheets["Top_Entities"] = pd.DataFrame(head_sample_rows)

        # Parameters sheet
        params_data = {
            "Parameter": ["Group By", "Value Column", "Time Column", "Thresholds"],
            "Value": [
                results.get("group_by", ""),
                results.get("value_column", ""),
                results.get("time_column", "none"),
                str(results.get("thresholds", [10, 20, 50])),
            ],
        }
        sheets["Parameters"] = pd.DataFrame(params_data)

        return StorageService.write_excel(
            sheets, output_path, with_formulas=include_formulas
        )

    @staticmethod
    def export_concentration_json(results: Dict[str, Any], output_path: Path) -> str:
        """
        Export concentration results to JSON.

        Args:
            results: Concentration analysis results
            output_path: Output file path

        Returns:
            Path to exported file
        """
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)

        return str(output_path)
