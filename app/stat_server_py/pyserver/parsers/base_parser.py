"""
Base parser with common utility methods for FHIR resource parsers.
"""

from typing import Any, Dict
import pandas as pd


class BaseParser:
    """Base class with common utility methods for FHIR parsers."""
    
    @staticmethod
    def _extract_cohort(row: pd.Series) -> str:
        """
        Extract cohort from meta.tag.
        
        Args:
            row: DataFrame row containing FHIR resource data
            
        Returns:
            Cohort code or 'Default' if not found
        """
        tags = row.get('resource.meta.tag')
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, dict) and tag.get('system') == 'urn:charm:cohort':
                    return tag.get('code', 'Default')
        return 'Default'
    
    @staticmethod
    def _extract_date(date_str: Any) -> str:
        """
        Extract and format date (just the date part, no time).
        
        Args:
            date_str: Date string in ISO format
            
        Returns:
            Date in YYYY-MM-DD format or None
        """
        if isinstance(date_str, str):
            # Extract just the date part (YYYY-MM-DD)
            if 'T' in date_str:
                return date_str.split('T')[0]
            return date_str
        return None
    
    @staticmethod
    def _extract_reference_info(reference: Any, display: Any = None) -> Dict[str, str]:
        """
        Extract ID and display from a FHIR reference.
        
        Args:
            reference: FHIR reference string (e.g., "Patient/123")
            display: Optional display name
            
        Returns:
            Dictionary with 'id' and 'display' keys
        """
        result = {'id': None, 'display': None}
        
        if isinstance(reference, str) and '/' in reference:
            result['id'] = reference.split('/')[-1]
        
        if isinstance(display, str):
            result['display'] = display
        
        return result
    
    @staticmethod
    def _extract_id_from_reference(reference: Any) -> str:
        """
        Extract just the ID from a FHIR reference.
        
        Args:
            reference: FHIR reference string (e.g., "Patient/123")
            
        Returns:
            ID portion of the reference or None
        """
        if isinstance(reference, str) and '/' in reference:
            return reference.split('/')[-1]
        return None
    
    @staticmethod
    def _extract_coding_value(coding_obj: Any, field: str = 'display') -> str:
        """
        Extract a value from a coding array.
        
        Args:
            coding_obj: FHIR coding array or object
            field: Field to extract ('display', 'code', 'system')
            
        Returns:
            Extracted value or None
        """
        if isinstance(coding_obj, list) and len(coding_obj) > 0:
            coding = coding_obj[0]
            if isinstance(coding, dict):
                return coding.get(field)
        return None
    
    @staticmethod
    def _extract_codeable_concept(codeable_concept: Any) -> Dict[str, str]:
        """
        Extract code, display, and system from a CodeableConcept.
        
        Args:
            codeable_concept: FHIR CodeableConcept object
            
        Returns:
            Dictionary with 'text', 'code', 'display', 'system' keys
        """
        result = {'text': None, 'code': None, 'display': None, 'system': None}
        
        if not isinstance(codeable_concept, dict):
            return result
        
        # Get text
        result['text'] = codeable_concept.get('text')
        
        # Get coding details
        coding = codeable_concept.get('coding')
        if isinstance(coding, list) and len(coding) > 0:
            code_obj = coding[0]
            if isinstance(code_obj, dict):
                result['code'] = code_obj.get('code')
                result['display'] = code_obj.get('display')
                result['system'] = code_obj.get('system')
        
        return result


