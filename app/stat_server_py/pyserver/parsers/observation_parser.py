"""
Parser for FHIR Observation resources.

Extracts clinically relevant fields from Observation resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class ObservationParser(BaseParser):
    """Parser for FHIR Observation resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Observation resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Observation resources
            
        Returns:
            DataFrame with clinically relevant Observation fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = ObservationParser._extract_cohort(row)
            
            # Basic identifiers
            parsed_row['status'] = row.get('resource.status')
            
            # Category
            parsed_row['category'] = ObservationParser._extract_category(row)
            
            # Code information (just the human-readable text)
            parsed_row['code_text'] = row.get('resource.code.text')
            
            # Encounter reference
            parsed_row['encounter_id'] = ObservationParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Dates
            parsed_row['effective_date'] = ObservationParser._extract_date(
                row.get('resource.effectiveDateTime')
            )
            parsed_row['issued'] = ObservationParser._extract_date(
                row.get('resource.issued')
            )
            
            # Value (combined value with unit for readability)
            value_info = ObservationParser._extract_value(row)
            parsed_row['value_with_unit'] = value_info.get('display')
            
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
                coding = cat.get('coding', [])
                if isinstance(coding, list) and len(coding) > 0:
                    code_obj = coding[0]
                    if isinstance(code_obj, dict):
                        return code_obj.get('display') or code_obj.get('code')
        return None
    
    @staticmethod
    def _extract_code(row: pd.Series) -> Dict[str, str]:
        """Extract code and system from code.coding."""
        code_obj = row.get('resource.code.coding')
        if isinstance(code_obj, list) and len(code_obj) > 0:
            coding = code_obj[0]
            if isinstance(coding, dict):
                return {
                    'code': coding.get('code'),
                    'system': coding.get('system')
                }
        return {'code': None, 'system': None}
    
    @staticmethod
    def _extract_value(row: pd.Series) -> Dict[str, Any]:
        """Extract value from various value[x] fields."""
        # Check for valueQuantity
        value_qty = row.get('resource.valueQuantity.value')
        if value_qty is not None:
            unit = row.get('resource.valueQuantity.unit') or row.get('resource.valueQuantity.code')
            return {
                'value': value_qty,
                'unit': unit,
                'display': f"{value_qty} {unit}" if unit else str(value_qty)
            }
        
        # Check for valueString
        value_str = row.get('resource.valueString')
        if value_str is not None:
            return {
                'value': value_str,
                'unit': None,
                'display': value_str
            }
        
        # Check for valueCodeableConcept
        value_cc = row.get('resource.valueCodeableConcept.text')
        if value_cc is not None:
            return {
                'value': value_cc,
                'unit': None,
                'display': value_cc
            }
        
        # Check for valueBoolean
        value_bool = row.get('resource.valueBoolean')
        if value_bool is not None:
            return {
                'value': value_bool,
                'unit': None,
                'display': str(value_bool)
            }
        
        return {
            'value': None,
            'unit': None,
            'display': None
        }

