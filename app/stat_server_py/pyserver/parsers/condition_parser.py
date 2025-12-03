"""
Parser for FHIR Condition resources.

Extracts clinically relevant fields from Condition resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class ConditionParser(BaseParser):
    """Parser for FHIR Condition resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Condition resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Condition resources
            
        Returns:
            DataFrame with clinically relevant Condition fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = ConditionParser._extract_cohort(row)
            
            # Clinical status
            parsed_row['clinical_status'] = ConditionParser._extract_coding_value(
                row.get('resource.clinicalStatus.coding'), field='code'
            )
            
            # Verification status
            parsed_row['verification_status'] = ConditionParser._extract_coding_value(
                row.get('resource.verificationStatus.coding'), field='code'
            )
            
            # Category
            parsed_row['category'] = ConditionParser._extract_coding_value(
                row.get('resource.category'), field='display'
            )
            
            # Condition code information - try flattened structure first
            parsed_row['condition_text'] = row.get('resource.code.text')
            
            # Extract from flattened coding array
            coding = row.get('resource.code.coding')
            if isinstance(coding, list) and len(coding) > 0:
                code_obj = coding[0]
                if isinstance(code_obj, dict):
                    parsed_row['condition_code'] = code_obj.get('code')
                    parsed_row['condition_system'] = code_obj.get('system')
                    # Use display if text is not available
                    if not parsed_row['condition_text']:
                        parsed_row['condition_text'] = code_obj.get('display')
            else:
                # Fallback: try as nested dict
                code_info = ConditionParser._extract_codeable_concept(row.get('resource.code'))
                parsed_row['condition_code'] = code_info.get('code')
                parsed_row['condition_system'] = code_info.get('system')
                if not parsed_row['condition_text']:
                    parsed_row['condition_text'] = code_info.get('text') or code_info.get('display')
            
            # Encounter reference
            parsed_row['encounter_id'] = ConditionParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Dates
            parsed_row['onset_date'] = ConditionParser._extract_date(
                row.get('resource.onsetDateTime')
            )
            parsed_row['recorded_date'] = ConditionParser._extract_date(
                row.get('resource.recordedDate')
            )
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

