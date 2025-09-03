"""
Application Settings
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional


class Settings(BaseSettings):
    """Application configuration."""

    # API Settings
    api_title: str = "Keye POC API"
    api_version: str = "0.1.0"
    api_prefix: str = "/api/v1"

    # Storage Settings
    storage_base_path: Path = Path("storage")
    datasets_path: Path = Path("storage/datasets")

    # LLM Settings
    use_llm: bool = True
    llm_provider: Optional[str] = "openai"  # openai, anthropic, etc.
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    llm_model: Optional[str] = "gpt-4o-mini"
    llm_temperature: float = 0.3
    llm_max_tokens: int = 2000

    # Security / API
    allowed_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:8000",
    ]
    api_key: Optional[str] = (
        "dev-key"  # if set, require X-API-Key header matching this value
    )

    # Analysis Settings
    default_thresholds: list[int] = [10, 20, 50]
    max_file_size_mb: int = 100
    allowed_extensions: list[str] = [".xlsx", ".xls", ".csv"]
    allowed_mime_types: list[str] = [
        "text/csv",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ]

    # Time Detection Settings
    year_range: tuple[int, int] = (1900, 2100)
    max_null_rate: float = 0.4
    time_validation_threshold: float = 0.7  # 70% of values must be valid for time detection
    year_range_min: int = 1900
    year_range_max: int = 2100

    # Normalization Settings
    currency_symbols: list[str] = ["$", "€", "£", "¥"]
    negative_allowed_columns: list[str] = [
        "discount",
        "returns",
        "adjustment",
        "delta",
        "change",
        "refund",
    ]

    # Development Settings
    debug: bool = False
    reload: bool = True

    # Pydantic v2-compatible settings configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    def get_dataset_path(self, dataset_id: str) -> Path:
        """Get the path for a specific dataset."""
        return self.datasets_path / dataset_id

    def get_normalized_path(self, dataset_id: str) -> Path:
        """Get the path for normalized data."""
        return self.get_dataset_path(dataset_id) / "normalized.parquet"

    def get_schema_path(self, dataset_id: str) -> Path:
        """Get the path for schema file."""
        return self.get_dataset_path(dataset_id) / "schema.json"

    def get_lineage_path(self, dataset_id: str) -> Path:
        """Get the path for lineage file."""
        return self.get_dataset_path(dataset_id) / "lineage.json"


# Singleton instance
settings = Settings()
