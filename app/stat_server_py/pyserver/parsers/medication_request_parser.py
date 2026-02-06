"""
Parser for FHIR MedicationRequest resources.

Extracts clinically relevant fields from MedicationRequest resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class MedicationRequestParser(BaseParser):
    """Parser for FHIR MedicationRequest resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of MedicationRequest resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR MedicationRequest resources
            
        Returns:
            DataFrame with clinically relevant MedicationRequest fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = MedicationRequestParser._extract_cohort(row)
            
            # Basic identifiers
            parsed_row['status'] = row.get('resource.status')
            parsed_row['intent'] = row.get('resource.intent')
            
            # Category
            parsed_row['category'] = MedicationRequestParser._extract_category(row)
            
            # Medication information (just the human-readable text)
            med_info = MedicationRequestParser._extract_medication(row)
            parsed_row['medication'] = med_info['text'] or med_info['display']
            
            # Encounter reference
            encounter_info = MedicationRequestParser._extract_reference_info(
                row.get('resource.encounter.reference')
            )
            parsed_row['encounter_id'] = encounter_info['id']
            
            # Dates
            parsed_row['authored_on'] = MedicationRequestParser._extract_date(
                row.get('resource.authoredOn')
            )
            
            # Dosage information
            dosage_info = MedicationRequestParser._extract_dosage(row)
            parsed_row['dosage_text'] = dosage_info['text']
            parsed_row['as_needed'] = dosage_info['as_needed']
            parsed_row['dose'] = dosage_info['dose']
            parsed_row['timing'] = dosage_info['timing']
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df
    
    @staticmethod
    def _extract_category(row: pd.Series) -> str:
        """Extract category display or code."""
        category = row.get('resource.category')
        if isinstance(category, list) and len(category) > 0:
            cat = category[0]
            if isinstance(cat, dict):
                # Try text first
                text = cat.get('text')
                if text:
                    return text
                # Then try coding
                coding = cat.get('coding', [])
                if isinstance(coding, list) and len(coding) > 0:
                    code_obj = coding[0]
                    if isinstance(code_obj, dict):
                        return code_obj.get('display') or code_obj.get('code')
        return None
    
    @staticmethod
    def _extract_medication(row: pd.Series) -> Dict[str, str]:
        """Extract medication information from medicationCodeableConcept."""
        result = {'text': None, 'code': None, 'display': None, 'system': None}
        
        # Try flattened structure first (how fhiry provides it)
        # Check for text field
        text = row.get('resource.medicationCodeableConcept.text')
        if text:
            result['text'] = text
        
        # Check for coding array
        coding = row.get('resource.medicationCodeableConcept.coding')
        if isinstance(coding, list) and len(coding) > 0:
            code_obj = coding[0]
            if isinstance(code_obj, dict):
                result['code'] = code_obj.get('code')
                result['display'] = code_obj.get('display')
                result['system'] = code_obj.get('system')
        
        # Fallback: try as a nested dict (for non-flattened data)
        if not any(result.values()):
            med_concept = row.get('resource.medicationCodeableConcept')
            if isinstance(med_concept, dict):
                return MedicationRequestParser._extract_codeable_concept(med_concept)
        
        return result
    
    @staticmethod
    def _extract_dosage(row: pd.Series) -> Dict[str, Any]:
        """Extract dosage information from dosageInstruction."""
        dosage = row.get('resource.dosageInstruction')
        result = {
            'text': None,
            'as_needed': None,
            'dose': None,
            'timing': None
        }
        
        if not isinstance(dosage, list) or len(dosage) == 0:
            return result
        
        # Get first dosage instruction
        dosage_inst = dosage[0]
        if not isinstance(dosage_inst, dict):
            return result
        
        # Dosage text
        result['text'] = dosage_inst.get('text')
        
        # As needed
        as_needed = dosage_inst.get('asNeededBoolean')
        if isinstance(as_needed, bool):
            result['as_needed'] = as_needed
        
        # Dose
        dose_and_rate = dosage_inst.get('doseAndRate')
        if isinstance(dose_and_rate, list) and len(dose_and_rate) > 0:
            dar = dose_and_rate[0]
            if isinstance(dar, dict):
                dose_qty = dar.get('doseQuantity', {})
                if isinstance(dose_qty, dict):
                    value = dose_qty.get('value')
                    unit = dose_qty.get('unit')
                    if value is not None:
                        if unit:
                            result['dose'] = f"{value} {unit}"
                        else:
                            result['dose'] = str(value)
        
        # Timing
        timing = dosage_inst.get('timing')
        if isinstance(timing, dict):
            repeat = timing.get('repeat', {})
            if isinstance(repeat, dict):
                frequency = repeat.get('frequency')
                period = repeat.get('period')
                period_unit = repeat.get('periodUnit')
                
                if all([frequency, period, period_unit]):
                    # Convert period_unit to human readable
                    unit_map = {
                        's': 'second(s)',
                        'min': 'minute(s)',
                        'h': 'hour(s)',
                        'd': 'day(s)',
                        'wk': 'week(s)',
                        'mo': 'month(s)',
                        'a': 'year(s)'
                    }
                    unit_display = unit_map.get(period_unit, period_unit)
                    result['timing'] = f"{frequency}x per {period} {unit_display}"
        
        return result

