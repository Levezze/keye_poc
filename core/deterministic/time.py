"""
Time Dimension Detection and Handling
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
import re
from datetime import datetime
from config.settings import settings


class TimeDetector:
    """Detects and handles time dimensions in data."""
    
    # Precompiled regex patterns for time detection
    _YEAR_PATTERN = re.compile(r'\b(year|yr|anno|année|ano)\b', re.IGNORECASE)
    _MONTH_PATTERN = re.compile(r'\b(month|mo|mois|mes|month_name)\b', re.IGNORECASE)
    _QUARTER_PATTERN = re.compile(r'\b(quarter|qtr|q[1-4]|trimestre|trim)\b', re.IGNORECASE)
    _DATE_PATTERN = re.compile(r'\b(date|dt|fecha|data|datetime|timestamp)', re.IGNORECASE)
    
    # Month name patterns
    _MONTH_NAMES = re.compile(r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b', re.IGNORECASE)
    
    def detect_time_dimensions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect time dimensions in DataFrame with precedence rules.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Dictionary with time dimension information including:
            - period_grain: Selected grain (date, year_month, year_quarter, year, none)
            - period_grain_candidates: All detected grains
            - time_candidates: All detected time columns
            - selected_time_columns: Dict of selected columns per grain
            - derivations: How period_key should be composed
            - warnings: Any issues detected
        """
        warnings = []
        time_candidates = {
            'date': [],
            'year': [],
            'month': [],
            'quarter': []
        }
        
        # Detect time columns by pattern matching and value validation
        for col in df.columns:
            col_str = str(col).lower()
            series = df[col]
            
            # Date detection (highest priority)
            if self._DATE_PATTERN.search(col_str):
                if self._validate_date_column(series):
                    time_candidates['date'].append(col)
            
            # Year detection
            elif self._YEAR_PATTERN.search(col_str):
                if self._validate_year_column(series):
                    time_candidates['year'].append(col)
            
            # Month detection
            elif self._MONTH_PATTERN.search(col_str) or self._has_month_names(series):
                if self._validate_month_column(series):
                    time_candidates['month'].append(col)
            
            # Quarter detection
            elif self._QUARTER_PATTERN.search(col_str):
                if self._validate_quarter_column(series):
                    time_candidates['quarter'].append(col)
        
        # Determine period grain with precedence: date > year+month > year+quarter > year > none
        period_grain, selected_columns, derivations = self._determine_period_grain(
            time_candidates, warnings
        )
        
        # Get all candidate grains
        period_grain_candidates = self._get_candidate_grains(time_candidates)
        
        return {
            "period_grain": period_grain,
            "period_grain_candidates": period_grain_candidates,
            "time_candidates": [col for cols in time_candidates.values() for col in cols],
            "selected_time_columns": selected_columns,
            "derivations": derivations,
            "warnings": warnings
        }
    
    def compose_period_key(
        self,
        df: pd.DataFrame,
        period_grain: str,
        derivations: Dict[str, Any]
    ) -> pd.Series:
        """
        Compose period key based on grain and derivations.
        
        Period key formats are designed for proper lexicographic sorting and clarity:
        - YYYY-MM-DD: Full ISO date format (2023-01-15) for date grain
        - YYYY-M##: Zero-padded month (2023-M01, 2023-M12) ensures proper sorting
          (avoids issues where "2023-M9" would sort after "2023-M10")
        - YYYY-Q#: Quarter with single digit (2023-Q1, 2023-Q4) - unambiguous
        - YYYY: Year only (2023) for annual periods
        - ALL: Single period indicator when no time dimension detected
        - UNKNOWN: Fallback for invalid/missing temporal data
        
        The format guarantees:
        1. Proper chronological sorting without custom comparators
        2. Human-readable period identification
        3. Consistent string length within each grain type
        4. Clear distinction between different time granularities
        
        Args:
            df: DataFrame with temporal columns
            period_grain: Type of period (date, year_month, year_quarter, year, none)
            derivations: Column information from detect_time_dimensions
        
        Returns:
            Series with formatted period keys for downstream analysis
        """
        if period_grain == "none":
            return pd.Series(["ALL"] * len(df), name="period_key")
        
        try:
            if period_grain == "date":
                date_col = derivations.get('date_column')
                if date_col:
                    dates = pd.to_datetime(df[date_col], errors='coerce')
                    return dates.dt.strftime('%Y-%m-%d').fillna('UNKNOWN')
            
            elif period_grain == "year_month":
                year_col = derivations.get('year_column')
                month_col = derivations.get('month_column')
                if year_col and month_col:
                    years = pd.to_numeric(df[year_col], errors='coerce')
                    months = self._normalize_months(df[month_col])
                    
                    # Handle NaN values properly in period key generation
                    result = pd.Series(['UNKNOWN'] * len(df), index=df.index)
                    
                    # Only format valid year+month combinations
                    valid_mask = years.notna() & months.notna()
                    if valid_mask.any():
                        valid_years = years[valid_mask].astype(int)
                        valid_months = months[valid_mask].astype(int)
                        result.loc[valid_mask] = (valid_years.astype(str) + '-M' + 
                                                 valid_months.astype(str).str.zfill(2))
                    
                    return result
            
            elif period_grain == "year_quarter":
                year_col = derivations.get('year_column')
                quarter_col = derivations.get('quarter_column')
                if year_col and quarter_col:
                    years = pd.to_numeric(df[year_col], errors='coerce')
                    quarters = self._normalize_quarters(df[quarter_col])
                    
                    # Handle NaN values properly in period key generation
                    result = pd.Series(['UNKNOWN'] * len(df), index=df.index)
                    
                    # Only format valid year+quarter combinations
                    valid_mask = years.notna() & quarters.notna()
                    if valid_mask.any():
                        valid_years = years[valid_mask].astype(int)
                        valid_quarters = quarters[valid_mask].astype(int)
                        result.loc[valid_mask] = (valid_years.astype(str) + '-Q' + 
                                                 valid_quarters.astype(str))
                    
                    return result
            
            elif period_grain == "year":
                year_col = derivations.get('year_column')
                if year_col:
                    years = pd.to_numeric(df[year_col], errors='coerce')
                    result = pd.Series(['UNKNOWN'] * len(df), index=df.index)
                    valid_mask = years.notna()
                    if valid_mask.any():
                        result.loc[valid_mask] = years[valid_mask].astype(int).astype(str)
                    return result
        
        except Exception:
            pass
        
        return pd.Series(["UNKNOWN"] * len(df), name="period_key")
    
    def _determine_period_grain(self, candidates: Dict[str, List], warnings: List[str]) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        """Determine period grain with precedence rules."""
        selected_columns = {}
        derivations = {}
        
        # Precedence: date > year+month > year+quarter > year > none
        if candidates['date']:
            if len(candidates['date']) > 1:
                warnings.append(f"Multiple date columns found: {candidates['date']}. Using first: {candidates['date'][0]}")
            selected_columns['date'] = candidates['date'][0]
            derivations['date_column'] = candidates['date'][0]
            return 'date', selected_columns, derivations
        
        elif candidates['year'] and candidates['month']:
            selected_columns['year'] = candidates['year'][0]
            selected_columns['month'] = candidates['month'][0]
            derivations['year_column'] = candidates['year'][0]
            derivations['month_column'] = candidates['month'][0]
            if len(candidates['year']) > 1:
                warnings.append(f"Multiple year columns found: {candidates['year']}. Using first.")
            if len(candidates['month']) > 1:
                warnings.append(f"Multiple month columns found: {candidates['month']}. Using first.")
            return 'year_month', selected_columns, derivations
        
        elif candidates['year'] and candidates['quarter']:
            selected_columns['year'] = candidates['year'][0]
            selected_columns['quarter'] = candidates['quarter'][0]
            derivations['year_column'] = candidates['year'][0]
            derivations['quarter_column'] = candidates['quarter'][0]
            if len(candidates['year']) > 1:
                warnings.append(f"Multiple year columns found: {candidates['year']}. Using first.")
            if len(candidates['quarter']) > 1:
                warnings.append(f"Multiple quarter columns found: {candidates['quarter']}. Using first.")
            return 'year_quarter', selected_columns, derivations
        
        elif candidates['year']:
            if len(candidates['year']) > 1:
                warnings.append(f"Multiple year columns found: {candidates['year']}. Using first: {candidates['year'][0]}")
            selected_columns['year'] = candidates['year'][0]
            derivations['year_column'] = candidates['year'][0]
            return 'year', selected_columns, derivations
        
        else:
            warnings.append("No temporal dimension detected")
            return 'none', {}, {}
    
    def _get_candidate_grains(self, candidates: Dict[str, List]) -> List[str]:
        """Get all possible grains based on detected candidates."""
        grains = []
        
        if candidates['date']:
            grains.append('date')
        if candidates['year'] and candidates['month']:
            grains.append('year_month')
        if candidates['year'] and candidates['quarter']:
            grains.append('year_quarter')
        if candidates['year']:
            grains.append('year')
        
        return grains if grains else ['none']
    
    def _validate_date_column(self, series: pd.Series) -> bool:
        """Validate if series contains valid dates."""
        if len(series) == 0:
            return False
            
        # Try to parse a sample of non-null values as dates
        non_null = series.dropna()
        if len(non_null) == 0:
            return False
        
        sample_size = min(10, len(non_null))
        sample = non_null.head(sample_size)
        
        try:
            parsed = pd.to_datetime(sample, errors='coerce')
            valid_count = parsed.notna().sum()
            return valid_count >= sample_size * settings.time_validation_threshold
        except:
            return False
    
    def _validate_year_column(self, series: pd.Series) -> bool:
        """Validate if series contains valid years (1900-2100)."""
        if len(series) == 0:
            return False
        
        non_null = series.dropna()
        if len(non_null) == 0:
            return False
        
        try:
            numeric_values = pd.to_numeric(non_null, errors='coerce')
            valid_years = numeric_values[(numeric_values >= settings.year_range_min) & (numeric_values <= settings.year_range_max)]
            return len(valid_years) >= len(non_null) * settings.time_validation_threshold
        except:
            return False
    
    def _validate_month_column(self, series: pd.Series) -> bool:
        """Validate if series contains valid months (1-12 or month names)."""
        if len(series) == 0:
            return False
        
        non_null = series.dropna()
        if len(non_null) == 0:
            return False
        
        valid_count = 0
        for val in non_null.head(10):  # Check first 10 values
            str_val = str(val).strip()
            
            # Check if it's a number 1-12
            try:
                num_val = int(float(str_val))
                if 1 <= num_val <= 12:
                    valid_count += 1
                    continue
            except:
                pass
            
            # Check if it's a month name
            if self._MONTH_NAMES.search(str_val):
                valid_count += 1
        
        return valid_count >= min(10, len(non_null)) * settings.time_validation_threshold
    
    def _validate_quarter_column(self, series: pd.Series) -> bool:
        """Validate if series contains valid quarters (1-4, Q1-Q4)."""
        if len(series) == 0:
            return False
        
        non_null = series.dropna()
        if len(non_null) == 0:
            return False
        
        valid_count = 0
        for val in non_null.head(10):
            str_val = str(val).strip().upper()
            
            # Check if it's a number 1-4
            try:
                num_val = int(float(str_val))
                if 1 <= num_val <= 4:
                    valid_count += 1
                    continue
            except:
                pass
            
            # Check if it's Q1-Q4 format
            if re.match(r'^Q[1-4]$', str_val):
                valid_count += 1
        
        return valid_count >= min(10, len(non_null)) * settings.time_validation_threshold
    
    def _has_month_names(self, series: pd.Series) -> bool:
        """Check if series contains month names."""
        non_null = series.dropna().head(10)
        for val in non_null:
            if self._MONTH_NAMES.search(str(val)):
                return True
        return False
    
    def _normalize_months(self, series: pd.Series) -> pd.Series:
        """Normalize months to numeric 1-12."""
        def normalize_month(val):
            if pd.isna(val):
                return np.nan
            
            str_val = str(val).strip().lower()
            
            # Try numeric first
            try:
                num_val = int(float(str_val))
                if 1 <= num_val <= 12:
                    return num_val
            except:
                pass
            
            # Month name mapping
            month_map = {
                'january': 1, 'jan': 1, 'february': 2, 'feb': 2,
                'march': 3, 'mar': 3, 'april': 4, 'apr': 4,
                'may': 5, 'june': 6, 'jun': 6, 'july': 7, 'jul': 7,
                'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
                'october': 10, 'oct': 10, 'november': 11, 'nov': 11,
                'december': 12, 'dec': 12
            }
            
            return month_map.get(str_val, np.nan)
        
        return series.apply(normalize_month)
    
    def _normalize_quarters(self, series: pd.Series) -> pd.Series:
        """Normalize quarters to numeric 1-4."""
        def normalize_quarter(val):
            if pd.isna(val):
                return np.nan
            
            str_val = str(val).strip().upper()
            
            # Try numeric first
            try:
                num_val = int(float(str_val))
                if 1 <= num_val <= 4:
                    return num_val
            except:
                pass
            
            # Q1-Q4 format
            match = re.match(r'^Q([1-4])$', str_val)
            if match:
                return int(match.group(1))
            
            return np.nan
        
        return series.apply(normalize_quarter)