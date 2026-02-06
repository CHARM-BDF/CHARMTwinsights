"""
Parser for FHIR DiagnosticReport resources.

Extracts clinically relevant fields from DiagnosticReport resources into a clean dataframe format.
Decodes base64-encoded report content.
"""

import pandas as pd
import base64
from typing import Any, Dict, List
from .base_parser import BaseParser


class DiagnosticReportParser(BaseParser):
    """Parser for FHIR DiagnosticReport resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of DiagnosticReport resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR DiagnosticReport resources
            
        Returns:
            DataFrame with clinically relevant DiagnosticReport fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = DiagnosticReportParser._extract_cohort(row)
            
            # Basic identifiers
            parsed_row['status'] = row.get('resource.status')
            
            # Category (may have multiple, take first display)
            parsed_row['category'] = DiagnosticReportParser._extract_category(row)
            
            # Code (type of report)
            parsed_row['code_text'] = DiagnosticReportParser._extract_code_text(row)
            
            # Encounter reference
            parsed_row['encounter_id'] = DiagnosticReportParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Dates
            parsed_row['effective_date'] = DiagnosticReportParser._extract_date(
                row.get('resource.effectiveDateTime')
            )
            parsed_row['issued'] = DiagnosticReportParser._extract_date(
                row.get('resource.issued')
            )
            
            # Performer (who created the report)
            parsed_row['performer'] = DiagnosticReportParser._extract_performer(row)
            
            # Decode base64 report content
            parsed_row['report_text'] = DiagnosticReportParser._extract_report_text(row)
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df
    
    @staticmethod
    def _extract_category(row: pd.Series) -> str:
        """Extract category display from first category coding."""
        category = row.get('resource.category')
        if isinstance(category, list) and len(category) > 0:
            first_category = category[0]
            if isinstance(first_category, dict):
                coding = first_category.get('coding')
                if isinstance(coding, list) and len(coding) > 0:
                    return coding[0].get('display') or coding[0].get('code')
        return None
    
    @staticmethod
    def _extract_code_text(row: pd.Series) -> str:
        """Extract code text or display from code field."""
        code = row.get('resource.code')
        if isinstance(code, dict):
            # Try text first
            text = code.get('text')
            if text:
                return text
            # Fall back to first coding display
            coding = code.get('coding')
            if isinstance(coding, list) and len(coding) > 0:
                return coding[0].get('display') or coding[0].get('code')
        return None
    
    @staticmethod
    def _extract_performer(row: pd.Series) -> str:
        """Extract performer display name from first performer."""
        performer = row.get('resource.performer')
        if isinstance(performer, list) and len(performer) > 0:
            first_performer = performer[0]
            if isinstance(first_performer, dict):
                return first_performer.get('display')
        return None
    
    @staticmethod
    def _extract_report_text(row: pd.Series) -> str:
        """Extract and decode base64-encoded report text from presentedForm."""
        presented_form = row.get('resource.presentedForm')
        if isinstance(presented_form, list) and len(presented_form) > 0:
            first_form = presented_form[0]
            if isinstance(first_form, dict):
                # Get base64 data
                data = first_form.get('data')
                if data:
                    try:
                        # Decode base64
                        decoded_bytes = base64.b64decode(data)
                        # Convert to string (assuming UTF-8)
                        decoded_text = decoded_bytes.decode('utf-8')
                        return decoded_text.strip()
                    except Exception as e:
                        return f"[Error decoding report: {str(e)}]"
        return None


