"""
Parser for FHIR Medication resources.

Extracts clinically relevant fields from Medication resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class MedicationParser(BaseParser):
    """Parser for FHIR Medication resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Medication resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Medication resources
            
        Returns:
            DataFrame with clinically relevant Medication fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = MedicationParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Medication information
            code_info = MedicationParser._extract_codeable_concept(row.get('resource.code'))
            parsed_row['medication'] = code_info.get('text') or code_info.get('display')
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

