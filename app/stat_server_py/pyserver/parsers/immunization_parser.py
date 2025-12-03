"""
Parser for FHIR Immunization resources.

Extracts clinically relevant fields from Immunization resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class ImmunizationParser(BaseParser):
    """Parser for FHIR Immunization resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Immunization resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Immunization resources
            
        Returns:
            DataFrame with clinically relevant Immunization fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = ImmunizationParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Vaccine information - try flattened structure first
            parsed_row['vaccine_text'] = row.get('resource.vaccineCode.text')
            
            # Extract from flattened coding array
            coding = row.get('resource.vaccineCode.coding')
            if isinstance(coding, list) and len(coding) > 0:
                code_obj = coding[0]
                if isinstance(code_obj, dict):
                    parsed_row['vaccine_code'] = code_obj.get('code')
                    parsed_row['vaccine_system'] = code_obj.get('system')
                    # Use display if text is not available
                    if not parsed_row['vaccine_text']:
                        parsed_row['vaccine_text'] = code_obj.get('display')
            else:
                # Fallback: try as nested dict
                vaccine_info = ImmunizationParser._extract_codeable_concept(
                    row.get('resource.vaccineCode')
                )
                parsed_row['vaccine_code'] = vaccine_info['code']
                parsed_row['vaccine_system'] = vaccine_info['system']
                if not parsed_row['vaccine_text']:
                    parsed_row['vaccine_text'] = vaccine_info['text'] or vaccine_info['display']
            
            # Encounter reference
            parsed_row['encounter_id'] = ImmunizationParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Occurrence date
            parsed_row['occurrence_date'] = ImmunizationParser._extract_date(
                row.get('resource.occurrenceDateTime')
            )
            
            # Location
            location_info = ImmunizationParser._extract_reference_info(
                row.get('resource.location.reference'),
                row.get('resource.location.display')
            )
            parsed_row['location'] = location_info['display']
            
            # Primary source
            parsed_row['primary_source'] = row.get('resource.primarySource')
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

