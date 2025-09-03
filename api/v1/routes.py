"""
API v1 Routes
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Header
from fastapi.responses import FileResponse
from typing import Optional, List
import pandas as pd
import tempfile
import os
import json
from pathlib import Path
import re

from config.settings import settings
from api.v1.models import (
    UploadResponse,
    ConcentrationRequest,
    ConcentrationResponse,
    SchemaResponse,
    InsightsResponse,
)
from services.registry import DatasetRegistry
from services.storage import StorageService
from services.normalization_service import NormalizationService
from core.deterministic.time import TimeDetector
from core.deterministic.concentration import ConcentrationAnalyzer
from services.exporters import ExportService
from services.exceptions import DatasetNotFoundError, SchemaNotFoundError

router = APIRouter()


def _require_api_key(x_api_key: Optional[str] = Header(default=None)):
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


_DATASET_ID_PATTERN = re.compile(r"^ds_[a-f0-9]{12}$")


def _validate_dataset_id(dataset_id: str) -> None:
    if not _DATASET_ID_PATTERN.match(dataset_id or ""):
        raise HTTPException(status_code=400, detail="Invalid dataset ID format")


@router.post("/upload", response_model=UploadResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    sheet: Optional[str] = None,
    x_api_key: Optional[str] = Header(default=None),
):
    """
    Upload an Excel or CSV file for analysis.

    Args:
        file: The file to upload (Excel or CSV)
        sheet: Optional sheet name for Excel files

    Returns:
        Dataset ID for subsequent operations
    """
    _require_api_key(x_api_key)

    # Basic validations
    # Size check (Content-Length) is not always present; enforce server-side max if provided by server middleware eventually
    filename = file.filename or ""
    ext = "." + filename.split(".")[-1].lower() if "." in filename else ""
    if ext not in settings.allowed_extensions:
        raise HTTPException(status_code=400, detail="Unsupported file extension")
    if file.content_type and file.content_type not in settings.allowed_mime_types:
        raise HTTPException(status_code=400, detail="Unsupported MIME type")

    # Initialize services
    registry = DatasetRegistry()
    storage = StorageService()
    norm_service = NormalizationService()
    time_detector = TimeDetector()

    try:
        # Create dataset
        dataset_id = registry.create_dataset(filename)

        # Save uploaded file
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp_file:
            content = await file.read()
            # Enforce file size limit
            max_bytes = settings.max_file_size_mb * 1024 * 1024
            if len(content) > max_bytes:
                raise HTTPException(status_code=413, detail="File too large")
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        try:
            # Load data
            if ext in [".xlsx", ".xls"]:
                if sheet is None:
                    # If no sheet specified, read the first sheet
                    df = pd.read_excel(tmp_file_path, sheet_name=0)
                else:
                    df = pd.read_excel(tmp_file_path, sheet_name=sheet)
            else:  # CSV
                df = pd.read_csv(tmp_file_path)

            if df.empty:
                raise HTTPException(status_code=400, detail="Uploaded file is empty")

            # Save raw file to dataset (using registry paths)
            dataset_path = settings.datasets_path / dataset_id
            raw_path = dataset_path / "raw" / filename
            with open(raw_path, "wb") as f:
                f.write(content)

            # Run normalization
            service_result = norm_service.normalize_and_persist(
                dataset_id, df, filename
            )
            norm_result = service_result["normalization_result"]

            # Detect time dimensions
            time_result = time_detector.detect_time_dimensions(norm_result.data)

            # Add time detection to schema
            schema = norm_result.schema.copy()
            schema.update(
                {
                    "period_grain": time_result["period_grain"],
                    "period_grain_candidates": time_result["period_grain_candidates"],
                    "time_candidates": time_result["time_candidates"],
                    "selected_time_columns": time_result["selected_time_columns"],
                    "derivations": time_result["derivations"],
                    "time_warnings": time_result["warnings"],
                }
            )

            # Save updated schema
            registry.save_schema(dataset_id, schema)

            # Record processing step
            registry.append_lineage_step(
                dataset_id,
                operation="upload_and_process",
                inputs=[filename],
                outputs=["normalized.parquet", "schema.json"],
                params={"sheet": sheet, "file_size": len(content)},
            )

            return UploadResponse(
                dataset_id=dataset_id,
                status="completed",
                message=f"Successfully processed {len(df)} rows with {len(df.columns)} columns",
                rows_processed=len(df),
                columns_processed=len(df.columns),
            )

        finally:
            # Clean up temp file
            os.unlink(tmp_file_path)

    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="File is empty or cannot be parsed")
    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail=f"File parsing error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


@router.get("/schema/{dataset_id}", response_model=SchemaResponse)
async def get_schema(dataset_id: str, x_api_key: Optional[str] = Header(default=None)):
    """
    Get the detected schema for a dataset.

    Args:
        dataset_id: The dataset identifier

    Returns:
        Schema information including column types and metadata
    """
    _require_api_key(x_api_key)

    registry = DatasetRegistry()

    try:
        # Check if dataset exists
        try:
            state = registry.get_dataset_state(dataset_id)
        except DatasetNotFoundError as e:
            # Correct behavior: 404 when dataset does not exist
            raise HTTPException(
                status_code=404, detail=str(e)
            )
        if not state["exists"]:
            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_id} not found"
            )

        # Get schema
        schema = registry.get_schema(dataset_id)
        if not schema:
            raise HTTPException(
                status_code=404, detail=f"Schema not found for dataset {dataset_id}"
            )

        # Convert to API response format
        return SchemaResponse(
            dataset_id=dataset_id,
            columns=schema.get("columns", []),
            period_grain=schema.get("period_grain", "none"),
            period_grain_candidates=schema.get("period_grain_candidates", []),
            time_candidates=schema.get("time_candidates", []),
            warnings=schema.get("warnings", []) + schema.get("time_warnings", []),
            notes=schema.get("notes", []),
            llm_insights=schema.get("llm_insights"),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error retrieving schema: {str(e)}"
        )


@router.post(
    "/analyze/{dataset_id}/concentration", response_model=ConcentrationResponse
)
async def analyze_concentration(
    dataset_id: str,
    request: ConcentrationRequest,
    background_tasks: BackgroundTasks,
    x_api_key: Optional[str] = Header(default=None),
):
    """
    Run concentration analysis on a dataset.

    Args:
        dataset_id: The dataset identifier
        request: Analysis parameters (group_by, value, time, thresholds)

    Returns:
        Concentration analysis results
    """
    _require_api_key(x_api_key)
    _validate_dataset_id(dataset_id)

    registry = DatasetRegistry()
    storage = StorageService()
    analyzer = ConcentrationAnalyzer()
    exporter = ExportService()
    time_detector = TimeDetector()

    try:
        # Check if dataset exists
        state = registry.get_dataset_state(dataset_id)
        if not state["exists"] or not state["has_normalized"]:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {dataset_id} not found or not normalized",
            )

        # Load normalized data
        dataset_path = settings.datasets_path / dataset_id
        df = storage.read_parquet(dataset_path / "normalized.parquet")

        # Get schema for time information
        schema = registry.get_schema(dataset_id)
        if not schema:
            raise HTTPException(status_code=404, detail="Schema not found")

        # Validate request parameters
        if request.group_by not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{request.group_by}' not found in dataset",
            )

        if request.value not in df.columns:
            raise HTTPException(
                status_code=400, detail=f"Column '{request.value}' not found in dataset"
            )

        # Add period key if time dimension exists
        period_key_column = None
        period_grain = schema.get("period_grain", "none")

        if period_grain != "none" and schema.get("derivations"):
            period_key = time_detector.compose_period_key(
                df, period_grain, schema["derivations"]
            )
            df["period_key"] = period_key
            period_key_column = "period_key"

        # Run concentration analysis
        analysis_result = analyzer.analyze(
            df=df,
            group_by=request.group_by,
            value_column=request.value,
            period_key_column=period_key_column,
            thresholds=request.thresholds or [10, 20, 50],
        )

        # Save results
        dataset_path = settings.datasets_path / dataset_id
        analysis_path = dataset_path / "analyses" / "concentration.json"
        with open(analysis_path, "w") as f:
            json.dump(analysis_result.data, f, indent=2, default=str)

        # Format data for exports (convert from analyzer format to export format)
        export_data = {"by_period": [], "details": []}

        for period_key, period_data in analysis_result.data.items():
            if period_key == "summary":
                continue
            concentration = period_data.get("concentration", {})
            period_export = {
                "period": period_key,
                "total": period_data.get("total_value", 0),
                "concentration": concentration,
            }
            export_data["by_period"].append(period_export)
            
            # Add head sample to details (limit to 10 items per period)
            head_sample = period_data.get("head_sample", [])[:10]
            for item in head_sample:
                detail_item = item.copy()
                detail_item["period"] = period_key
                export_data["details"].append(detail_item)

        # Add totals data for single-period export fallback
        if "TOTAL" in analysis_result.data:
            totals_data = analysis_result.data["TOTAL"]
            export_data["totals"] = {
                "total": totals_data.get("total_value", 0),
                "concentration": totals_data.get("concentration", {}),
            }
        
        # Add export metadata
        export_data.update({
            "group_by": request.group_by,
            "value_column": request.value,
            "time_column": period_key_column or "none",
            "thresholds": request.thresholds or [10, 20, 50],
        })

        # Generate exports
        analyses_path = dataset_path / "analyses"
        csv_path = exporter.export_concentration_csv(
            export_data, analyses_path / "concentration.csv"
        )
        excel_path = exporter.export_concentration_excel(
            export_data, analyses_path / "concentration.xlsx"
        )
        export_paths = {"csv": csv_path, "xlsx": excel_path}

        # Record analysis step
        registry.append_lineage_step(
            dataset_id,
            operation="concentration_analysis",
            inputs=["normalized.parquet"],
            outputs=[
                "analyses/concentration.json",
                "analyses/concentration.csv",
                "analyses/concentration.xlsx",
            ],
            params={
                "group_by": request.group_by,
                "value": request.value,
                "period_grain": period_grain,
                "thresholds": request.thresholds or [10, 20, 50],
            },
            metrics={"computation_steps": len(analysis_result.computation_log)},
        )

        # Format response
        by_period = []
        totals = {}

        for period_key, period_data in analysis_result.data.items():
            if period_key == "summary":
                continue

            concentration = period_data.get("concentration", {})

            # Convert concentration metrics to API format
            def convert_concentration_metric(metric_data):
                if not metric_data:
                    return None
                return {
                    "count": metric_data.get("count", 0),
                    "value": metric_data.get("value", 0.0),
                    "pct_of_total": metric_data.get("percentage", 0.0),
                }

            # Get head sample (limit to 10 items for API payload size)
            head_sample = period_data.get("head_sample", [])[:10]
            
            # Build dynamic concentration metrics from analyzer results
            concentration_metrics = {}
            for threshold_key, metric_data in concentration.items():
                concentration_metrics[threshold_key] = convert_concentration_metric(metric_data)
            
            period_result = {
                "period": period_key,
                "total": period_data.get("total_value", 0),
                "concentration": concentration_metrics,
                "head": head_sample,
            }

            if period_key == "TOTAL" or period_key == "ALL":
                totals = {
                    "period": period_key,
                    "total_entities": period_data.get("total_entities", 0),
                    "total_value": period_data.get("total_value", 0),
                    "concentration": concentration,
                }
            else:
                by_period.append(period_result)

        # Convert computation log to string warnings
        warnings = [
            f"{entry.get('step', 'step')}: {entry.get('message', str(entry))}"
            for entry in analysis_result.computation_log[-5:]
        ]

        return ConcentrationResponse(
            dataset_id=dataset_id,
            period_grain=period_grain,
            warnings=warnings,
            thresholds=request.thresholds or [10, 20, 50],
            by_period=by_period,
            totals=totals,
            export_links={
                "csv": f"/api/v1/download/{dataset_id}/concentration.csv",
                "xlsx": f"/api/v1/download/{dataset_id}/concentration.xlsx",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis error: {str(e)}")


@router.get("/download/{dataset_id}/concentration.csv")
async def download_concentration_csv(
    dataset_id: str, x_api_key: Optional[str] = Header(default=None)
):
    """
    Download concentration analysis results as CSV.

    Args:
        dataset_id: The dataset identifier

    Returns:
        CSV file download
    """
    _require_api_key(x_api_key)
    # Accept any dataset_id here; return 404 if not found

    registry = DatasetRegistry()
    storage = StorageService()

    try:
        # Check if dataset and analysis exist
        state = registry.get_dataset_state(dataset_id)
        if not state["exists"] or not state["has_analyses"]:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {dataset_id} or concentration analysis not found",
            )

        dataset_path = settings.datasets_path / dataset_id
        csv_path = dataset_path / "analyses" / "concentration.csv"

        if not csv_path.exists():
            raise HTTPException(
                status_code=404,
                detail="CSV export file not found. Run concentration analysis first.",
            )

        # Read CSV content so we can append dimension metadata for POC UX
        # TODO: TECH DEBT - Replace post-append metadata with proper CSV structure
        # Current: Appends "GroupBy,{value}" line after CSV data  
        # Better: Add metadata as proper CSV columns or separate metadata.csv file
        # Tracked in: docs/tech_debt.md
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

        # Append group_by info so the CSV includes the dimension name (e.g., 'entity')
        group_by_value = None
        lineage = registry.get_lineage(dataset_id)
        if lineage:
            for step in reversed(lineage.get("steps", [])):
                if step.get("operation") == "concentration_analysis":
                    group_by_value = step.get("params", {}).get("group_by")
                    break

        if group_by_value:
            content = content.rstrip("\n") + f"\nGroupBy,{group_by_value}\n"

        from fastapi import Response

        return Response(content=content, media_type="text/csv; charset=utf-8")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")


@router.get("/download/{dataset_id}/concentration.xlsx")
async def download_concentration_excel(
    dataset_id: str, x_api_key: Optional[str] = Header(default=None)
):
    """
    Download concentration analysis results as Excel.

    Args:
        dataset_id: The dataset identifier

    Returns:
        Excel file download
    """
    _require_api_key(x_api_key)
    _validate_dataset_id(dataset_id)

    registry = DatasetRegistry()
    storage = StorageService()

    try:
        # Check if dataset and analysis exist
        state = registry.get_dataset_state(dataset_id)
        if not state["exists"] or not state["has_analyses"]:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {dataset_id} or concentration analysis not found",
            )

        dataset_path = settings.datasets_path / dataset_id
        xlsx_path = dataset_path / "analyses" / "concentration.xlsx"

        if not xlsx_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Excel export file not found. Run concentration analysis first.",
            )

        return FileResponse(
            path=str(xlsx_path),
            filename=f"{dataset_id}_concentration.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")


@router.get("/insights/{dataset_id}", response_model=InsightsResponse)
async def get_insights(
    dataset_id: str, x_api_key: Optional[str] = Header(default=None)
):
    """
    Get AI-generated insights for a dataset's analysis.

    Args:
        dataset_id: The dataset identifier

    Returns:
        AI-generated insights and recommendations
    """
    _require_api_key(x_api_key)
    _validate_dataset_id(dataset_id)

    registry = DatasetRegistry()
    storage = StorageService()

    try:
        # Check if dataset exists
        state = registry.get_dataset_state(dataset_id)
        if not state["exists"]:
            raise HTTPException(
                status_code=404, detail=f"Dataset {dataset_id} not found"
            )

        # Get schema and concentration analysis if available
        schema = registry.get_schema(dataset_id)
        dataset_path = settings.datasets_path / dataset_id
        analysis_path = dataset_path / "analyses" / "concentration.json"

        key_findings = []
        recommendations = []

        if schema:
            # Basic data insights
            num_columns = len(schema.get("columns", []))
            period_grain = schema.get("period_grain", "none")

            key_findings.append(
                f"Dataset contains {num_columns} columns with {period_grain} time granularity"
            )

            # Time dimension insights
            if period_grain != "none":
                recommendations.append(
                    f"Leverage {period_grain} time series for trend analysis"
                )
            else:
                recommendations.append(
                    "Consider adding temporal dimensions for time-based analysis"
                )

        if analysis_path.exists():
            # Load concentration analysis
            with open(analysis_path, "r") as f:
                analysis_data = json.load(f)

            # Generate insights from concentration analysis
            if "TOTAL" in analysis_data or "ALL" in analysis_data:
                total_data = analysis_data.get("TOTAL", analysis_data.get("ALL", {}))
                concentration = total_data.get("concentration", {})

                if "top_10" in concentration:
                    top_10 = concentration["top_10"]
                    key_findings.append(
                        f"Top 10%: {top_10['count']} entities = {top_10['percentage']:.1f}% of value"
                    )

                    if top_10["percentage"] > 80:
                        recommendations.append(
                            "High concentration risk - consider diversification strategies"
                        )

        # Default content if no analysis
        if not key_findings:
            key_findings.append(
                "Run concentration analysis to generate detailed insights"
            )

        return InsightsResponse(
            dataset_id=dataset_id,
            executive_summary=f"Analysis summary for dataset {dataset_id} with {len(key_findings)} key findings",
            key_findings=key_findings,
            risk_indicators=(
                ["Concentration analysis pending"] if not analysis_path.exists() else []
            ),
            opportunities=(
                ["Enhanced analytics available with time series"]
                if schema and schema.get("period_grain") != "none"
                else []
            ),
            recommendations=recommendations,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Insights generation error: {str(e)}"
        )


@router.get("/lineage/{dataset_id}")
async def get_lineage(dataset_id: str, x_api_key: Optional[str] = Header(default=None)):
    """
    Get the complete audit trail for a dataset.

    Args:
        dataset_id: The dataset identifier

    Returns:
        Complete lineage and audit trail
    """
    _require_api_key(x_api_key)
    _validate_dataset_id(dataset_id)
    # TODO: Implement lineage retrieval
    return {"dataset_id": dataset_id, "created_at": None, "steps": []}
