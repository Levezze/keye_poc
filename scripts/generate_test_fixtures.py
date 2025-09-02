#!/usr/bin/env python
"""
Generate Test Fixtures from Excel Files

This script creates sampled test fixtures from large Excel files for testing purposes.
It generates both Excel and CSV versions with various sampling strategies.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import argparse
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import hashlib
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from services.storage import StorageService


class TestFixtureGenerator:
    """Generate test fixtures from real data files."""
    
    def __init__(self, source_file: Path, output_dir: Path, seed: Optional[int] = 42):
        """
        Initialize fixture generator.
        
        Args:
            source_file: Path to source Excel file
            output_dir: Output directory for fixtures
            seed: Random seed for reproducibility
        """
        self.source_file = Path(source_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set random seed for reproducibility
        np.random.seed(seed)
        self.seed = seed
        
        # Load source data
        print(f"Loading source file: {self.source_file}")
        self.df = pd.read_excel(self.source_file)
        self.original_shape = self.df.shape
        print(f"Loaded {self.original_shape[0]} rows, {self.original_shape[1]} columns")
        
        # Detect column types
        self._detect_columns()
        
    def _detect_columns(self):
        """Detect time, group, and value columns."""
        self.time_column = None
        self.group_columns = []
        self.value_columns = []
        
        for col in self.df.columns:
            # Check if it's a time column
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                self.time_column = col
            elif col.lower() in ['date', 'period', 'month', 'quarter', 'year']:
                self.time_column = col
            # Check if it's a categorical/group column
            elif pd.api.types.is_object_dtype(self.df[col]) or \
                 pd.api.types.is_categorical_dtype(self.df[col]):
                if self.df[col].nunique() < len(self.df) * 0.5:  # Less than 50% unique
                    self.group_columns.append(col)
            # Check if it's a numeric value column
            elif pd.api.types.is_numeric_dtype(self.df[col]):
                self.value_columns.append(col)
        
        print(f"Detected time column: {self.time_column}")
        print(f"Detected group columns: {self.group_columns[:3]}...")
        print(f"Detected value columns: {self.value_columns[:3]}...")
    
    def random_sample(self, percentage: float = 20.0) -> pd.DataFrame:
        """
        Create a random sample of the data.
        
        Args:
            percentage: Percentage of rows to sample
        
        Returns:
            Sampled DataFrame
        """
        sample_size = int(len(self.df) * (percentage / 100))
        return self.df.sample(n=sample_size, random_state=self.seed)
    
    def time_balanced_sample(self, rows_per_period: int = 10) -> pd.DataFrame:
        """
        Create a sample that preserves all time periods.
        
        Args:
            rows_per_period: Number of rows to sample per time period
        
        Returns:
            Sampled DataFrame
        """
        if not self.time_column:
            print("No time column detected, falling back to random sample")
            return self.random_sample(20)
        
        # Group by time period and sample from each
        sampled_dfs = []
        for period, group in self.df.groupby(self.time_column):
            if len(group) <= rows_per_period:
                sampled_dfs.append(group)
            else:
                sampled_dfs.append(group.sample(n=rows_per_period, random_state=self.seed))
        
        return pd.concat(sampled_dfs, ignore_index=True)
    
    def top_entities_sample(self, top_n: int = 10) -> pd.DataFrame:
        """
        Sample top N entities by total value.
        
        Args:
            top_n: Number of top entities to include
        
        Returns:
            Sampled DataFrame
        """
        if not self.group_columns or not self.value_columns:
            print("No group/value columns detected, falling back to random sample")
            return self.random_sample(20)
        
        # Use first group and value column
        group_col = self.group_columns[0]
        value_col = self.value_columns[0]
        
        # Calculate totals per entity
        entity_totals = self.df.groupby(group_col)[value_col].sum().sort_values(ascending=False)
        top_entities = entity_totals.head(top_n).index
        
        # Filter for top entities
        return self.df[self.df[group_col].isin(top_entities)]
    
    def edge_cases_sample(self, force_nulls: bool = True) -> pd.DataFrame:
        """
        Create a sample focusing on edge cases.
        
        Args:
            force_nulls: If True, artificially add nulls if none exist in source data
        
        Returns:
            DataFrame with edge cases including nulls
        """
        edge_cases = []
        
        # Add rows with null values (existing nulls from source data)
        null_mask = self.df.isnull().any(axis=1)
        if null_mask.any():
            edge_cases.append(self.df[null_mask].head(5))
        
        # Add rows with extreme values for each numeric column
        for col in self.value_columns[:3]:  # Limit to first 3 value columns
            if pd.api.types.is_numeric_dtype(self.df[col]):
                # Min values
                edge_cases.append(self.df.nsmallest(2, col))
                # Max values
                edge_cases.append(self.df.nlargest(2, col))
        
        # Add rows with special characters in text columns
        for col in self.group_columns[:2]:  # Limit to first 2 group columns
            if pd.api.types.is_object_dtype(self.df[col]):
                # Look for special characters
                special_mask = self.df[col].astype(str).str.contains(r'[^\w\s]', regex=True, na=False)
                if special_mask.any():
                    edge_cases.append(self.df[special_mask].head(3))
        
        # Create base edge cases DataFrame
        if edge_cases:
            edge_df = pd.concat(edge_cases, ignore_index=True).drop_duplicates()
        else:
            print("No natural edge cases found, using random sample as base")
            edge_df = self.random_sample(10)
        
        # If no nulls exist and force_nulls is True, add some artificial nulls
        if force_nulls and not edge_df.isnull().any().any():
            print("Adding artificial nulls to edge cases sample")
            # Add random nulls to ensure edge case testing includes null handling
            edge_df = self.add_random_nulls(edge_df, null_percentage=15, preserve_key_columns=True)
        
        return edge_df
    
    def stratified_sample(self, percentage: float = 20.0) -> pd.DataFrame:
        """
        Create a stratified sample maintaining data distribution.
        
        Args:
            percentage: Percentage to sample
        
        Returns:
            Stratified sample DataFrame
        """
        if not self.group_columns:
            return self.random_sample(percentage)
        
        # Use first group column for stratification
        group_col = self.group_columns[0]
        
        # Calculate sample size per group
        group_sizes = self.df[group_col].value_counts()
        sample_sizes = (group_sizes * (percentage / 100)).round().astype(int)
        sample_sizes = sample_sizes[sample_sizes > 0]  # Remove groups with 0 samples
        
        # Sample from each group
        sampled_dfs = []
        for group_value, sample_size in sample_sizes.items():
            group_df = self.df[self.df[group_col] == group_value]
            if len(group_df) <= sample_size:
                sampled_dfs.append(group_df)
            else:
                sampled_dfs.append(group_df.sample(n=sample_size, random_state=self.seed))
        
        return pd.concat(sampled_dfs, ignore_index=True)
    
    def add_random_nulls(self, df: pd.DataFrame, null_percentage: float = 10.0, 
                        columns: Optional[list] = None, preserve_key_columns: bool = True) -> pd.DataFrame:
        """
        Add random null values to a DataFrame.
        
        Args:
            df: DataFrame to add nulls to
            null_percentage: Percentage of values to make null in each target column
            columns: Optional list of columns to add nulls to (default: all columns)
            preserve_key_columns: Whether to preserve key columns (time, primary group) from nulls
        
        Returns:
            DataFrame with added null values
        """
        df_with_nulls = df.copy()
        
        # Determine which columns to add nulls to
        target_columns = columns if columns else df.columns.tolist()
        
        # Remove key columns if preserve_key_columns is True
        if preserve_key_columns:
            exclude = []
            # Preserve time column
            if self.time_column and self.time_column in target_columns:
                exclude.append(self.time_column)
            # Preserve first group column (like customer ID)
            if self.group_columns and self.group_columns[0] in target_columns:
                exclude.append(self.group_columns[0])
            
            target_columns = [c for c in target_columns if c not in exclude]
        
        # Add nulls to each target column
        for col in target_columns:
            if len(df) == 0:
                continue
            
            n_nulls = max(1, int(len(df) * null_percentage / 100))  # At least 1 null
            n_nulls = min(n_nulls, len(df))  # Don't exceed available rows
            
            null_indices = np.random.choice(df.index, size=n_nulls, replace=False)
            df_with_nulls.loc[null_indices, col] = np.nan
        
        return df_with_nulls
    
    def add_single_column_nulls(self, df: pd.DataFrame, column: str, 
                               null_percentage: float = 20.0, 
                               pattern: str = 'random') -> pd.DataFrame:
        """
        Add null values to a single column with configurable patterns.
        
        Args:
            df: DataFrame to add nulls to
            column: Name of the column to add nulls to
            null_percentage: Percentage of values in the column to make null
            pattern: Pattern for adding nulls ('random', 'first_n', 'last_n', 'every_nth')
        
        Returns:
            DataFrame with nulls added to the specified column
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        df_with_nulls = df.copy()
        n_nulls = max(1, int(len(df) * null_percentage / 100))  # At least 1 null
        n_nulls = min(n_nulls, len(df))  # Don't exceed available rows
        
        if pattern == 'random':
            null_indices = np.random.choice(df.index, size=n_nulls, replace=False)
        elif pattern == 'first_n':
            null_indices = df.index[:n_nulls]
        elif pattern == 'last_n':
            null_indices = df.index[-n_nulls:]
        elif pattern == 'every_nth':
            step = max(1, len(df) // n_nulls)
            null_indices = df.index[::step][:n_nulls]
        else:
            raise ValueError(f"Unknown pattern '{pattern}'. Use 'random', 'first_n', 'last_n', or 'every_nth'")
        
        df_with_nulls.loc[null_indices, column] = np.nan
        return df_with_nulls
    
    def save_sample(self, df: pd.DataFrame, name: str, description: str) -> Dict[str, Any]:
        """
        Save sample in both Excel and CSV formats.
        
        Args:
            df: DataFrame to save
            name: Base name for the file
            description: Description of the sampling strategy
        
        Returns:
            Metadata about the saved files
        """
        # Ensure sample_data subdirectory exists
        sample_dir = self.output_dir / "sample_data"
        sample_dir.mkdir(exist_ok=True)
        
        # Save Excel version
        excel_path = sample_dir / f"{name}.xlsx"
        excel_checksum = StorageService.write_excel(df, excel_path)
        excel_size = excel_path.stat().st_size
        
        # Save CSV version
        csv_path = sample_dir / f"{name}.csv"
        csv_checksum = StorageService.write_csv(df, csv_path)
        csv_size = csv_path.stat().st_size
        
        print(f"  ✓ {name}.xlsx ({excel_size / 1024:.1f} KB)")
        print(f"  ✓ {name}.csv ({csv_size / 1024:.1f} KB)")
        
        return {
            "name": name,
            "description": description,
            "rows": len(df),
            "columns": len(df.columns),
            "excel": {
                "path": str(excel_path.relative_to(self.output_dir)),
                "size_bytes": excel_size,
                "checksum": excel_checksum
            },
            "csv": {
                "path": str(csv_path.relative_to(self.output_dir)),
                "size_bytes": csv_size,
                "checksum": csv_checksum
            }
        }
    
    def generate_all_fixtures(self) -> Dict[str, Any]:
        """
        Generate all fixture variations.
        
        Returns:
            Metadata about all generated fixtures
        """
        print("\nGenerating test fixtures...")
        metadata = {
            "source_file": str(self.source_file),
            "source_shape": self.original_shape,
            "generated_at": datetime.now().isoformat(),
            "seed": self.seed,
            "detected_columns": {
                "time": self.time_column,
                "groups": self.group_columns[:5],  # Limit to first 5
                "values": self.value_columns[:5]   # Limit to first 5
            },
            "fixtures": []
        }
        
        # 1. Random 20% sample
        print("\n1. Creating 20% random sample...")
        df_20pct = self.random_sample(20)
        metadata["fixtures"].append(
            self.save_sample(df_20pct, "sample_20pct", "20% random sample for general testing")
        )
        
        # 2. Small 5% sample
        print("\n2. Creating 5% small sample...")
        df_small = self.random_sample(5)
        metadata["fixtures"].append(
            self.save_sample(df_small, "sample_small", "5% random sample for quick unit tests")
        )
        
        # 3. Time-balanced sample
        if self.time_column:
            print("\n3. Creating time-balanced sample...")
            df_time = self.time_balanced_sample(10)
            metadata["fixtures"].append(
                self.save_sample(df_time, "sample_time_balanced", 
                               "All time periods with 10 rows each")
            )
        
        # 4. Top entities sample
        if self.group_columns and self.value_columns:
            print("\n4. Creating top entities sample...")
            df_top = self.top_entities_sample(10)
            metadata["fixtures"].append(
                self.save_sample(df_top, "sample_top_entities", 
                               "Top 10 entities by value with all their data")
            )
        
        # 5. Edge cases sample
        print("\n5. Creating edge cases sample...")
        df_edge = self.edge_cases_sample()
        metadata["fixtures"].append(
            self.save_sample(df_edge, "sample_edge_cases", 
                           "Edge cases including nulls, extremes, and special characters")
        )
        
        # 6. Stratified sample
        if self.group_columns:
            print("\n6. Creating stratified sample...")
            df_stratified = self.stratified_sample(15)
            metadata["fixtures"].append(
                self.save_sample(df_stratified, "sample_stratified", 
                               "15% stratified sample maintaining group distribution")
            )
        
        # 7. Sample with random nulls across columns
        print("\n7. Creating sample with random nulls...")
        df_with_nulls = self.add_random_nulls(self.random_sample(10), null_percentage=12)
        metadata["fixtures"].append(
            self.save_sample(df_with_nulls, "sample_with_nulls", 
                           "10% sample with 12% random nulls across columns (excluding keys)")
        )
        
        # 8. Sample with nulls in single column (first value column if available)
        if self.value_columns:
            print("\n8. Creating sample with single-column nulls...")
            df_single_null = self.add_single_column_nulls(
                self.random_sample(8), 
                column=self.value_columns[0], 
                null_percentage=25
            )
            metadata["fixtures"].append(
                self.save_sample(df_single_null, "sample_single_null_column", 
                               f"8% sample with 25% nulls in '{self.value_columns[0]}' column")
            )
        
        # Save metadata
        metadata_path = self.output_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"\n✓ Metadata saved to {metadata_path}")
        
        return metadata


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate test fixtures from Excel files"
    )
    parser.add_argument(
        "source_file",
        type=Path,
        help="Path to source Excel file"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tests/fixtures"),
        help="Output directory for fixtures (default: tests/fixtures)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    
    args = parser.parse_args()
    
    # Validate source file
    if not args.source_file.exists():
        print(f"Error: Source file {args.source_file} does not exist")
        return 1
    
    if not args.source_file.suffix.lower() in ['.xlsx', '.xls']:
        print(f"Error: Source file must be an Excel file (.xlsx or .xls)")
        return 1
    
    # Generate fixtures
    try:
        generator = TestFixtureGenerator(
            source_file=args.source_file,
            output_dir=args.output_dir,
            seed=args.seed
        )
        
        metadata = generator.generate_all_fixtures()
        
        print("\n" + "=" * 60)
        print("✅ Test fixtures generated successfully!")
        print(f"📁 Output directory: {args.output_dir}")
        print(f"📊 Generated {len(metadata['fixtures'])} fixture sets")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error generating fixtures: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())