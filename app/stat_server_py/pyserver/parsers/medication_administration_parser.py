"""
Parser for FHIR MedicationAdministration resources.

Extracts clinically relevant fields from MedicationAdministration resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class MedicationAdministrationParser(BaseParser):
    """Parser for FHIR MedicationAdministration resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of MedicationAdministration resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR MedicationAdministration resources
            
        Returns:
            DataFrame with clinically relevant MedicationAdministration fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = MedicationAdministrationParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Medication information - try flattened structure first
            medication_text = row.get('resource.medicationCodeableConcept.text')
            
            # Extract from flattened coding array
            coding = row.get('resource.medicationCodeableConcept.coding')
            if isinstance(coding, list) and len(coding) > 0:
                code_obj = coding[0]
                if isinstance(code_obj, dict):
                    medication_display = code_obj.get('display')
                    parsed_row['medication'] = medication_text or medication_display
            else:
                # Fallback: try as nested dict
                med_info = MedicationAdministrationParser._extract_codeable_concept(
                    row.get('resource.medicationCodeableConcept')
                )
                parsed_row['medication'] = med_info['text'] or med_info['display']
            
            # If we got text from flattened structure, use it
            if medication_text and 'medication' not in parsed_row:
                parsed_row['medication'] = medication_text
            
            # Context (encounter)
            parsed_row['encounter_id'] = MedicationAdministrationParser._extract_id_from_reference(
                row.get('resource.context.reference')
            )
            
            # Effective date
            parsed_row['effective_date'] = MedicationAdministrationParser._extract_date(
                row.get('resource.effectiveDateTime')
            )
            
            # Reason
            reason_refs = row.get('resource.reasonReference')
            if isinstance(reason_refs, list) and len(reason_refs) > 0:
                first_reason = reason_refs[0]
                if isinstance(first_reason, dict):
                    parsed_row['reason'] = first_reason.get('display')
            else:
                parsed_row['reason'] = None
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

