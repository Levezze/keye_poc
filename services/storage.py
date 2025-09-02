"""
Storage Service
Handles file I/O operations for various formats.
"""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, Union, Dict, Any, List
import hashlib
import xlsxwriter
from io import BytesIO


class StorageService:
    """Handles storage operations for datasets."""
    
    @staticmethod
    def read_excel(
        file_path: Union[str, Path],
        sheet_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Read Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Optional sheet name
        
        Returns:
            DataFrame
        """
        return pd.read_excel(file_path, sheet_name=sheet_name or 0)
    
    @staticmethod
    def read_csv(
        file_path: Union[str, Path],
        **kwargs
    ) -> pd.DataFrame:
        """
        Read CSV file.
        
        Args:
            file_path: Path to CSV file
            **kwargs: Additional pandas read_csv arguments
        
        Returns:
            DataFrame
        """
        return pd.read_csv(file_path, **kwargs)
    
    @staticmethod
    def write_parquet(
        df: pd.DataFrame,
        file_path: Union[str, Path],
        **kwargs
    ) -> str:
        """
        Write DataFrame to Parquet.
        
        Args:
            df: DataFrame to write
            file_path: Output path
            **kwargs: Additional parquet write arguments
        
        Returns:
            SHA256 checksum of the file
        """
        file_path = Path(file_path)
        df.to_parquet(file_path, index=False, **kwargs)
        
        # Calculate checksum
        return StorageService.calculate_checksum(file_path)
    
    @staticmethod
    def read_parquet(
        file_path: Union[str, Path],
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Read Parquet file.
        
        Args:
            file_path: Path to Parquet file
            columns: Optional columns to read
        
        Returns:
            DataFrame
        """
        return pd.read_parquet(file_path, columns=columns)
    
    @staticmethod
    def write_csv(
        df: pd.DataFrame,
        file_path: Union[str, Path],
        **kwargs
    ) -> str:
        """
        Write DataFrame to CSV.
        
        Args:
            df: DataFrame to write
            file_path: Output path
            **kwargs: Additional CSV write arguments
        
        Returns:
            Path to written file
        """
        file_path = Path(file_path)
        df.to_csv(file_path, index=False, **kwargs)
        return str(file_path)
    
    @staticmethod
    def write_excel(
        data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        file_path: Union[str, Path],
        with_formulas: bool = False
    ) -> str:
        """
        Write DataFrame(s) to Excel.
        
        Args:
            data: DataFrame or dict of sheet_name -> DataFrame
            file_path: Output path
            with_formulas: Whether to include formulas
        
        Returns:
            Path to written file
        """
        file_path = Path(file_path)
        
        if isinstance(data, pd.DataFrame):
            data = {"Sheet1": data}
        
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            for sheet_name, df in data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                if with_formulas:
                    # TODO: Add formula generation for audit
                    pass
        
        return str(file_path)
    
    @staticmethod
    def calculate_checksum(file_path: Union[str, Path]) -> str:
        """
        Calculate SHA256 checksum of a file.
        
        Args:
            file_path: Path to file
        
        Returns:
            SHA256 hex digest
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    @staticmethod
    def save_upload(
        file_content: bytes,
        file_path: Union[str, Path]
    ) -> str:
        """
        Save uploaded file content.
        
        Args:
            file_content: File content as bytes
            file_path: Destination path
        
        Returns:
            SHA256 checksum
        """
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, "wb") as f:
            f.write(file_content)
        
        return StorageService.calculate_checksum(file_path)