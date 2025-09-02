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
    def export_concentration_csv(
        results: Dict[str, Any],
        output_path: Path
    ) -> str:
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
        
        if "by_period" in results:
            for period_data in results["by_period"]:
                period = period_data["period"]
                for threshold in ["top_10", "top_20", "top_50"]:
                    if threshold in period_data:
                        metrics = period_data[threshold]
                        rows.append({
                            "period": period,
                            "threshold": threshold.replace("top_", ""),
                            "count": metrics.get("count", 0),
                            "value": metrics.get("value", 0),
                            "pct_of_total": metrics.get("pct_of_total", 0)
                        })
        
        df = pd.DataFrame(rows)
        return StorageService.write_csv(df, output_path)
    
    @staticmethod
    def export_concentration_excel(
        results: Dict[str, Any],
        output_path: Path,
        include_formulas: bool = True
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
        if "by_period" in results:
            for period_data in results["by_period"]:
                row = {"period": period_data["period"], "total": period_data["total"]}
                for threshold in ["top_10", "top_20", "top_50"]:
                    if threshold in period_data:
                        metrics = period_data[threshold]
                        row[f"{threshold}_count"] = metrics.get("count", 0)
                        row[f"{threshold}_value"] = metrics.get("value", 0)
                        row[f"{threshold}_pct"] = metrics.get("pct_of_total", 0)
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
                str(results.get("thresholds", [10, 20, 50]))
            ]
        }
        sheets["Parameters"] = pd.DataFrame(params_data)
        
        return StorageService.write_excel(sheets, output_path, with_formulas=include_formulas)
    
    @staticmethod
    def export_concentration_json(
        results: Dict[str, Any],
        output_path: Path
    ) -> str:
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