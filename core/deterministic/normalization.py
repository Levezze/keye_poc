"""
Data Normalization Module
Handles deterministic data cleaning and normalization with comprehensive tracking.
"""
import pandas as pd
import numpy as np
import re
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
from config.settings import settings


@dataclass
class NormalizationResult:
    """Result of normalization process."""
    data: pd.DataFrame
    transformations: List[Dict[str, Any]]
    anomalies: List[Dict[str, Any]]
    statistics: Dict[str, Any]
    schema: Dict[str, Any]
    header_mapping: Dict[str, str]
    warnings: List[str]


class DataNormalizer:
    """Handles data normalization and cleaning."""
    
    def normalize(self, df: pd.DataFrame) -> NormalizationResult:
        """
        Normalize a DataFrame with deterministic rules.
        
        Args:
            df: Input DataFrame
        
        Returns:
            NormalizationResult with cleaned data and metadata
        """
        transformations = []
        warnings = []
        
        # 1. Standardize headers
        df_normalized, header_mapping = self._standardize_headers(df.copy())
        transformations.append({
            "step": "header_standardization",
            "original_columns": list(df.columns),
            "standardized_columns": list(df_normalized.columns)
        })
        
        # 2. Detect and coerce types with comprehensive tracking
        df_normalized, type_transformations, type_warnings = self._detect_and_coerce_types(df_normalized)
        transformations.extend(type_transformations)
        warnings.extend(type_warnings)
        
        # 3. Apply domain rules
        domain_results = self._apply_domain_rules(df_normalized)
        warnings.extend(domain_results['warnings'])
        
        # 4. Detect anomalies
        anomalies = self._detect_anomalies(df_normalized)
        
        # 5. Generate schema
        schema = self._generate_schema(
            df_normalized, header_mapping, transformations, warnings, anomalies
        )
        
        statistics = {
            "rows_in": len(df),
            "rows_out": len(df_normalized),
            "columns_in": len(df.columns),
            "columns_out": len(df_normalized.columns),
            "total_transformations": len(transformations),
            "warnings_count": len(warnings)
        }
        
        return NormalizationResult(
            data=df_normalized,
            transformations=transformations,
            anomalies=anomalies,
            statistics=statistics,
            schema=schema,
            header_mapping=header_mapping,
            warnings=warnings
        )
    
    def _standardize_headers(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Standardize column headers with original mapping."""
        mapping = {}
        new_columns = []
        seen = set()
        
        for col in df.columns:
            # Store original
            original = col
            
            # Clean: trim, lowercase, replace special chars with underscores
            clean = str(col).strip().lower()
            # Replace spaces and special chars with underscores
            clean = re.sub(r'[^\w]', '_', clean)
            # Collapse multiple underscores
            clean = re.sub(r'_+', '_', clean).strip('_')
            
            # Handle duplicates
            if clean in seen:
                i = 2
                while f"{clean}_{i}" in seen:
                    i += 1
                clean = f"{clean}_{i}"
            
            seen.add(clean)
            new_columns.append(clean)
            mapping[clean] = original  # standardized -> original
        
        df.columns = new_columns
        return df, mapping
    
    def _detect_and_coerce_types(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Detect and coerce column types with comprehensive tracking."""
        transformations = []
        warnings = []
        
        for col in df.columns:
            original_dtype = str(df[col].dtype)
            
            # Try numeric coercion first
            numeric_result, numeric_counters = self._coerce_numeric(df[col], col)
            if numeric_counters['successful_coercions'] > 0:
                df[col] = numeric_result
                
                # Check for percent representation even after successful numeric coercion
                _, percent_counters = self._normalize_percentages(df[col], col)
                if percent_counters.get('representation') == 'percent':
                    numeric_counters['representation'] = 'percent'
                
                transformations.append({
                    "step": "numeric_coercion",
                    "column": col,
                    "original_dtype": original_dtype,
                    "new_dtype": "float64",
                    "counters": numeric_counters
                })
                
                # Check for mixed decimal conventions
                if numeric_counters.get('decimal_convention') == 'mixed':
                    warnings.append(f"Column '{col}' contains mixed decimal conventions (both US and EU formats)")
                
                continue
            
            # Try percent normalization
            percent_result, percent_counters = self._normalize_percentages(df[col], col)
            if percent_counters['percent_normalized'] > 0:
                df[col] = percent_result
                transformations.append({
                    "step": "percent_normalization", 
                    "column": col,
                    "original_dtype": original_dtype,
                    "new_dtype": "float64",
                    "counters": percent_counters
                })
                continue
            elif percent_counters.get('representation') == 'percent':
                # Even if no normalization occurred, record percent representation
                transformations.append({
                    "step": "percent_representation", 
                    "column": col,
                    "original_dtype": original_dtype,
                    "new_dtype": original_dtype,
                    "counters": percent_counters
                })
                continue
            
            # Try datetime coercion
            datetime_result, datetime_counters = self._coerce_datetime(df[col], col)
            if datetime_counters['datetime_parsed'] > 0:
                df[col] = datetime_result
                transformations.append({
                    "step": "datetime_coercion",
                    "column": col,
                    "original_dtype": original_dtype,
                    "new_dtype": "datetime64[ns]",
                    "counters": datetime_counters
                })
                continue
                
            # Try boolean coercion
            boolean_result, boolean_counters = self._coerce_boolean(df[col], col)
            if boolean_counters['boolean_coerced'] > 0:
                df[col] = boolean_result
                transformations.append({
                    "step": "boolean_coercion",
                    "column": col,
                    "original_dtype": original_dtype,
                    "new_dtype": "bool",
                    "counters": boolean_counters
                })
        
        return df, transformations, warnings
    
    def _coerce_numeric(self, series: pd.Series, col_name: str) -> Tuple[pd.Series, Dict[str, Any]]:
        """Coerce series to numeric with comprehensive tracking."""
        counters = {
            'successful_coercions': 0,
            'currency_removed': 0,
            'parentheses_to_negative': 0,
            'scaling_applied': 0,
            'unicode_minus_normalized': 0,
            'failed_numeric_coercions': 0,
            'decimal_convention': None,
            'decimal_conventions_seen': set(),
            'currencies_detected': set(),
            'multi_currency': False,
            'representation': None
        }
        
        def parse_value(val):
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return np.nan
                
            val_str = str(val).strip()
            original_val = val_str
            
            # Skip if already numeric
            try:
                return float(val_str)
            except:
                pass
            
            # Detect Unicode minus
            if '\u2212' in val_str:
                val_str = val_str.replace('\u2212', '-')
                counters['unicode_minus_normalized'] += 1
            
            # Detect currency symbols (including in text like CHF)
            currency_matches = re.findall(r'[\$€£¥]', val_str)
            if currency_matches:
                for curr in currency_matches:
                    counters['currencies_detected'].add(curr)
                val_str = re.sub(r'[\$€£¥]', '', val_str)
                counters['currency_removed'] += 1
            
            # Handle currency codes like CHF, USD, EUR
            if re.search(r'\b(CHF|USD|EUR|GBP|JPY)\b', val_str):
                code_match = re.search(r'\b(CHF|USD|EUR|GBP|JPY)\b', val_str)
                if code_match:
                    counters['currencies_detected'].add(code_match.group())
                    val_str = re.sub(r'\b(CHF|USD|EUR|GBP|JPY)\b', '', val_str)
                    counters['currency_removed'] += 1
            
            # Handle parentheses for negatives
            if val_str.strip().startswith('(') and val_str.strip().endswith(')'):
                val_str = '-' + val_str.strip()[1:-1]
                counters['parentheses_to_negative'] += 1
            
            # Handle trailing minus
            if val_str.strip().endswith('-'):
                val_str = '-' + val_str.strip()[:-1]
            
            # Handle scaling suffixes
            scale_map = {
                'k': 1000, 'K': 1000,
                'm': 1000000, 'M': 1000000, 'mm': 1000000,
                'b': 1000000000, 'B': 1000000000, 'bn': 1000000000
            }
            
            scale_applied = False
            for suffix, multiplier in scale_map.items():
                pattern = r'(\d+(?:[.,]\d+)?)\s*' + re.escape(suffix) + r'\s*$'
                match = re.search(pattern, val_str.strip(), re.IGNORECASE)
                if match:
                    val_str = match.group(1)
                    scale_applied = multiplier
                    counters['scaling_applied'] += 1
                    break
            
            # Remove spaces (including non-breaking) and apostrophes as thousands separators
            val_str = re.sub(r'[\s\u00A0\u202F\']', '', val_str)
            
            # Determine decimal convention and parse
            try:
                # Handle mixed . and , 
                if '.' in val_str and ',' in val_str:
                    # Rightmost is decimal separator
                    if val_str.rindex('.') > val_str.rindex(','):
                        # US style: 1,234.56
                        val_str = val_str.replace(',', '')
                        counters['decimal_conventions_seen'].add('US')
                    else:
                        # EU style: 1.234,56
                        val_str = val_str.replace('.', '').replace(',', '.')
                        counters['decimal_conventions_seen'].add('EU')
                elif ',' in val_str:
                    # Check if comma is decimal (1-2 digits after)
                    if re.search(r',\d{1,2}$', val_str):
                        val_str = val_str.replace(',', '.')
                        counters['decimal_conventions_seen'].add('EU')
                    else:
                        val_str = val_str.replace(',', '')
                        counters['decimal_conventions_seen'].add('US')
                
                result = float(val_str)
                if scale_applied:
                    result *= scale_applied
                
                counters['successful_coercions'] += 1
                return result
                
            except (ValueError, AttributeError):
                counters['failed_numeric_coercions'] += 1
                return np.nan
        
        result = series.apply(parse_value)
        
        # Determine final decimal convention
        if len(counters['decimal_conventions_seen']) == 0:
            counters['decimal_convention'] = None
        elif len(counters['decimal_conventions_seen']) == 1:
            counters['decimal_convention'] = list(counters['decimal_conventions_seen'])[0]
        else:
            counters['decimal_convention'] = 'mixed'
        
        # Set multi-currency flag
        if len(counters['currencies_detected']) > 1:
            counters['multi_currency'] = True
            
        # Convert sets to lists for JSON serialization
        counters['currencies_detected'] = list(counters['currencies_detected'])
        counters['decimal_conventions_seen'] = list(counters['decimal_conventions_seen'])
        
        return result, counters
    
    def _normalize_percentages(self, series: pd.Series, col_name: str) -> Tuple[pd.Series, Dict[str, Any]]:
        """Normalize percentages with header heuristics."""
        counters = {
            'percent_normalized': 0,
            'representation': None
        }
        
        # Check header for percent hint - be more permissive
        has_pct_header = bool(re.search(r'(?i)(percent|pct|percentage|%|rate|ratio|margin)', col_name))
        
        def parse_percent(val):
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return val
                
            val_str = str(val).strip()
            
            # Check for % symbol
            if val_str.endswith('%'):
                try:
                    num = float(val_str[:-1])
                    counters['percent_normalized'] += 1
                    return num / 100.0
                except:
                    return val
            
            # If header suggests percent and value looks like percentage
            if has_pct_header:
                try:
                    # Handle both string and numeric inputs
                    if isinstance(val, (int, float)):
                        num = float(val)
                    else:
                        num = float(val_str)
                    
                    # Only normalize if value > 1 (don't rescale values already in [0,1])
                    if 1 < num <= 100:
                        counters['percent_normalized'] += 1
                        return num / 100.0
                except:
                    pass
            
            return val
        
        result = series.apply(parse_percent)
        
        # Set representation if header suggests percent or if normalization occurred
        if counters['percent_normalized'] > 0 or has_pct_header:
            counters['representation'] = 'percent'
            
        return result, counters
    
    def _coerce_datetime(self, series: pd.Series, col_name: str) -> Tuple[pd.Series, Dict[str, Any]]:
        """Coerce to datetime with explicit locale handling."""
        counters = {
            'datetime_parsed': 0,
            'parsing_errors': 0
        }
        
        # Check if column name suggests datetime - be more permissive 
        is_date_column = bool(re.search(
            r'(?i)(date|dt|time|timestamp|created|updated|modified)', 
            col_name
        ))
        
        if not is_date_column:
            return series, counters
        
        def parse_date(val):
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return val
                
            try:
                # Try pandas to_datetime with explicit settings
                result = pd.to_datetime(val, errors='coerce', dayfirst=False)
                if pd.notna(result):
                    counters['datetime_parsed'] += 1
                    return result
                else:
                    counters['parsing_errors'] += 1
                    return np.nan  # Return NaN for failed parsing
            except:
                counters['parsing_errors'] += 1
                return np.nan  # Return NaN for exceptions
        
        result = series.apply(parse_date)
        return result, counters
    
    def _coerce_boolean(self, series: pd.Series, col_name: str) -> Tuple[pd.Series, Dict[str, Any]]:
        """Coerce to boolean values - be conservative to avoid corrupting numeric data."""
        counters = {
            'boolean_coerced': 0
        }
        
        # Only attempt boolean coercion if column appears to be boolean-like
        # Check if most values are boolean-like
        sample_size = min(100, len(series))
        sample_values = series.dropna().head(sample_size)
        
        if len(sample_values) == 0:
            return series, counters
        
        # Count how many values look boolean-like
        boolean_like_count = 0
        for val in sample_values:
            val_str = str(val).strip().lower()
            if val_str in ['yes', 'no', 'y', 'n', 'true', 'false', 't', 'f', '1', '0']:
                boolean_like_count += 1
            elif isinstance(val, bool):
                boolean_like_count += 1
        
        # Only proceed if majority of values are boolean-like
        boolean_ratio = boolean_like_count / len(sample_values)
        if boolean_ratio < 0.7:  # At least 70% must be boolean-like
            return series, counters
        
        # Boolean mapping - only for clearly boolean values
        bool_map = {
            # String values
            'yes': True, 'no': False,
            'y': True, 'n': False,
            'true': True, 'false': False,
            't': True, 'f': False,
        }
        
        def parse_bool(val):
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return val
                
            # If already boolean, keep it
            if isinstance(val, bool):
                counters['boolean_coerced'] += 1
                return val
                
            val_str = str(val).strip().lower()
            
            if val_str in bool_map:
                counters['boolean_coerced'] += 1
                return bool_map[val_str]
            
            # Only coerce 1/0 if they are strings, not numeric
            if val_str in ['1', '0'] and isinstance(val, str):
                counters['boolean_coerced'] += 1
                return val_str == '1'
            
            return val
        
        result = series.apply(parse_bool)
        return result, counters
    
    def _apply_domain_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Apply domain-specific rules with corrected negative policy."""
        # CORRECTED: revenue NOT in allowlist - always flag negative revenues
        # Merge hardcoded financial terms with configurable settings
        financial_negative_allowed = ['gross_profit', 'net_income', 'cost', 'expense', 'adjustments', 
                                    'margin', 'ebitda', 'ebit', 'profit', 'loss']
        negative_allowed = financial_negative_allowed + settings.negative_allowed_columns
        
        warnings = []
        anomalies = {}
        metadata = {
            'multi_currency_columns': [],
            'mixed_decimal_conventions': []
        }
        
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                neg_count = (df[col] < 0).sum()
                total_count = df[col].notna().sum()
                
                if neg_count > 0:
                    anomalies[col] = {
                        'negative_count': int(neg_count),
                        'negative_rate': float(neg_count / total_count) if total_count > 0 else 0.0
                    }
                    
                    # Flag negative revenues and other unexpected negatives
                    is_negative_allowed = any(allowed in col.lower() for allowed in negative_allowed)
                    if not is_negative_allowed:
                        warnings.append(
                            f"Unexpected negative values in '{col}': {neg_count} occurrences"
                        )
        
        return {
            'warnings': warnings,
            'anomalies': anomalies,
            'metadata': metadata
        }
    
    def _detect_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect data anomalies with comprehensive statistics."""
        anomalies = {}
        
        for col in df.columns:
            col_anomalies = {}
            
            # Null rate
            null_count = df[col].isna().sum()
            total_count = len(df[col])
            null_rate = null_count / total_count if total_count > 0 else 0.0
            
            col_anomalies['null_count'] = int(null_count)
            col_anomalies['null_rate'] = float(null_rate)
            
            # High null rate flag
            if null_rate > 0.5:
                col_anomalies['high_null_rate'] = True
            
            # For numeric columns, detect outliers
            if df[col].dtype in ['float64', 'int64'] and df[col].notna().sum() > 0:
                numeric_data = df[col].dropna()
                if len(numeric_data) > 0:
                    mean_val = numeric_data.mean()
                    std_val = numeric_data.std()
                    
                    if std_val > 0:
                        outliers = numeric_data[np.abs(numeric_data - mean_val) > 3 * std_val]
                        if len(outliers) > 0:
                            col_anomalies['outlier_count'] = len(outliers)
                            col_anomalies['outlier_rate'] = float(len(outliers) / len(numeric_data))
            
            # Cardinality
            cardinality = df[col].nunique()
            col_anomalies['cardinality'] = int(cardinality)
            
            # Low cardinality flag for large datasets
            if len(df) > 100 and cardinality < 5:
                col_anomalies['low_cardinality'] = True
            
            anomalies[col] = col_anomalies
        
        return anomalies
    
    def _generate_schema(self, df: pd.DataFrame, header_mapping: Dict[str, str], 
                        transformations: List[Dict[str, Any]], warnings: List[str],
                        anomalies: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive schema with full metadata."""
        columns_info = []
        
        # Extract coercion counters from transformations
        coercion_data = {}
        for trans in transformations:
            if 'counters' in trans:
                col = trans['column']
                coercion_data[col] = trans['counters']
        
        for col in df.columns:
            # Determine role
            if df[col].dtype in ['float64', 'int64']:
                cardinality = df[col].nunique()
                total_rows = len(df[col])
                # Consider numeric if high cardinality OR if it's clearly a numeric column
                # Use a more flexible threshold based on data size
                threshold = min(50, max(5, total_rows // 10))  # Adaptive threshold
                role = 'numeric' if cardinality > threshold or cardinality > total_rows * 0.5 else 'categorical'
            elif df[col].dtype == 'datetime64[ns]':
                role = 'datetime'
            elif df[col].dtype == 'bool':
                role = 'categorical'
            else:
                role = 'categorical'
            
            # Get coercion counters for this column
            col_coercions = coercion_data.get(col, {})
            
            # Build column info
            col_info = {
                'name': col,
                'original_name': header_mapping.get(col, col),
                'dtype': str(df[col].dtype),
                'role': role,
                'cardinality': int(df[col].nunique()),
                'null_rate': float(df[col].isna().sum() / len(df)),
                'coercions': {
                    'currency_removed': col_coercions.get('currency_removed', 0),
                    'parentheses_to_negative': col_coercions.get('parentheses_to_negative', 0),
                    'scaling_applied': col_coercions.get('scaling_applied', 0),
                    'unicode_minus_normalized': col_coercions.get('unicode_minus_normalized', 0),
                    'percent_normalized': col_coercions.get('percent_normalized', 0),
                    'datetime_parsed': col_coercions.get('datetime_parsed', 0),
                    'boolean_coerced': col_coercions.get('boolean_coerced', 0),
                    'failed_numeric_coercions': col_coercions.get('failed_numeric_coercions', 0)
                },
                'representation': col_coercions.get('representation'),
                'anomalies': anomalies.get(col, {})
            }
            
            # Add decimal convention if available
            if 'decimal_convention' in col_coercions:
                col_info['decimal_convention'] = col_coercions['decimal_convention']
            
            # Add currency information if available
            if 'currencies_detected' in col_coercions:
                col_info['currencies_detected'] = col_coercions['currencies_detected']
            if 'multi_currency' in col_coercions:
                col_info['multi_currency'] = col_coercions['multi_currency']
            
            columns_info.append(col_info)
        
        # Check for multi-currency at dataset level
        all_currencies = set()
        for col_data in coercion_data.values():
            if 'currencies_detected' in col_data:
                all_currencies.update(col_data['currencies_detected'])
        
        schema = {
            'dataset_id': None,  # Will be set by caller
            'generated_at': datetime.now().isoformat(),
            'columns': columns_info,
            'metadata': {
                'row_count': len(df),
                'column_count': len(df.columns),
                'multi_currency': len(all_currencies) > 1,
                'currencies_detected': list(all_currencies),
                'has_time_dimension': any(col['role'] == 'datetime' for col in columns_info)
            },
            'warnings': warnings,
            'transformations_summary': {
                'total_transformations': len(transformations),
                'columns_modified': len(coercion_data),
                'transformation_types': list(set(t['step'] for t in transformations if 'step' in t))
            }
        }
        
        return schema