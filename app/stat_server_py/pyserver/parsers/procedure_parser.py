"""
Parser for FHIR Procedure resources.

Extracts clinically relevant fields from Procedure resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class ProcedureParser(BaseParser):
    """Parser for FHIR Procedure resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Procedure resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Procedure resources
            
        Returns:
            DataFrame with clinically relevant Procedure fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = ProcedureParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Code information
            parsed_row['procedure_text'] = row.get('resource.code.text')
            code_info = ProcedureParser._extract_code(row)
            parsed_row['procedure_code'] = code_info.get('code')
            
            # Encounter reference
            parsed_row['encounter_id'] = ProcedureParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Performed period
            parsed_row['performed_start'] = ProcedureParser._extract_date(
                row.get('resource.performedPeriod.start')
            )
            parsed_row['performed_end'] = ProcedureParser._extract_date(
                row.get('resource.performedPeriod.end')
            )
            
            # Location
            location_info = ProcedureParser._extract_reference_info(
                row.get('resource.location.reference'),
                row.get('resource.location.display')
            )
            parsed_row['location'] = location_info['display']
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df
    
    @staticmethod
    def _extract_code(row: pd.Series) -> Dict[str, str]:
        """Extract code and system from coding array."""
        coding = row.get('resource.code.coding')
        if isinstance(coding, list) and len(coding) > 0:
            first_coding = coding[0]
            if isinstance(first_coding, dict):
                return {
                    'code': first_coding.get('code'),
                    'system': first_coding.get('system')
                }
        return {'code': None, 'system': None}


