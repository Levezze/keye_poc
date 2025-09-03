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
        # Convert results to DataFrame format
        rows = []

        if "by_period" in results and len(results["by_period"]) > 0:
            for period_data in results["by_period"]:
                period = period_data["period"]
                # Get concentration data and iterate over actual thresholds
                concentration = period_data.get("concentration", {})
                for threshold_key, metrics in concentration.items():
                    if isinstance(metrics, dict):
                        # Extract threshold number from key (e.g., "top_10" -> "10")
                        threshold_display = threshold_key.replace("top_", "")
                        rows.append(
                            {
                                "period": period,
                                "threshold": threshold_display,
                                "count": metrics.get("count", 0),
                                "value": metrics.get("value", 0),
                                "pct_of_total": round(metrics.get("percentage", 0), 1),
                            }
                        )
        elif "totals" in results:
            # Handle single-period case (no time dimension)
            totals_data = results["totals"]
            concentration = totals_data.get("concentration", {})
            for threshold_key, metrics in concentration.items():
                if isinstance(metrics, dict):
                    # Extract threshold number from key (e.g., "top_10" -> "10")
                    threshold_display = threshold_key.replace("top_", "")
                    rows.append(
                        {
                            "period": "TOTAL",
                            "threshold": threshold_display,
                            "count": metrics.get("count", 0),
                            "value": metrics.get("value", 0),
                            "pct_of_total": round(metrics.get("percentage", 0), 1),
                        }
                    )

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
        summary_rows = []
        if "by_period" in results and len(results["by_period"]) > 0:
            for period_data in results["by_period"]:
                row = {"period": period_data["period"], "total": period_data["total"]}
                # Get concentration data and iterate over actual thresholds
                concentration = period_data.get("concentration", {})
                for threshold_key, metrics in concentration.items():
                    if isinstance(metrics, dict):
                        row[f"{threshold_key}_count"] = metrics.get("count", 0)
                        row[f"{threshold_key}_value"] = metrics.get("value", 0)
                        row[f"{threshold_key}_pct"] = round(metrics.get("percentage", 0), 1)
                summary_rows.append(row)
        elif "totals" in results:
            # Handle single-period case (no time dimension)
            totals_data = results["totals"]
            row = {"period": "TOTAL", "total": totals_data.get("total", 0)}
            concentration = totals_data.get("concentration", {})
            for threshold_key, metrics in concentration.items():
                if isinstance(metrics, dict):
                    row[f"{threshold_key}_count"] = metrics.get("count", 0)
                    row[f"{threshold_key}_value"] = metrics.get("value", 0)
                    row[f"{threshold_key}_pct"] = round(metrics.get("pct_of_total", 0), 1)
            summary_rows.append(row)

        if summary_rows:
            sheets["Summary"] = pd.DataFrame(summary_rows)

        # Details sheet (if available)
        if "details" in results:
            sheets["Details"] = pd.DataFrame(results["details"])

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
