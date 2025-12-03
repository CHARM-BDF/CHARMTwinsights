"""
Parser for FHIR DocumentReference resources.

Extracts clinically relevant fields from DocumentReference resources into a clean dataframe format.
Decodes base64-encoded document content.
"""

import pandas as pd
import base64
from typing import Any, Dict, List
from .base_parser import BaseParser


class DocumentReferenceParser(BaseParser):
    """Parser for FHIR DocumentReference resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of DocumentReference resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR DocumentReference resources
            
        Returns:
            DataFrame with clinically relevant DocumentReference fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = DocumentReferenceParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Type
            parsed_row['type'] = DocumentReferenceParser._extract_type(row)
            
            # Category
            parsed_row['category'] = DocumentReferenceParser._extract_category(row)
            
            # Date
            parsed_row['date'] = DocumentReferenceParser._extract_date(
                row.get('resource.date')
            )
            
            # Author (practitioner)
            parsed_row['author'] = DocumentReferenceParser._extract_author(row)
            
            # Custodian (organization)
            custodian_info = DocumentReferenceParser._extract_reference_info(
                row.get('resource.custodian.reference'),
                row.get('resource.custodian.display')
            )
            parsed_row['custodian'] = custodian_info['display']
            
            # Encounter reference
            parsed_row['encounter_id'] = DocumentReferenceParser._extract_encounter_id(row)
            
            # Period
            parsed_row['period_start'] = DocumentReferenceParser._extract_date(
                row.get('resource.context.period.start')
            )
            parsed_row['period_end'] = DocumentReferenceParser._extract_date(
                row.get('resource.context.period.end')
            )
            
            # Decode base64 content
            parsed_row['document_text'] = DocumentReferenceParser._extract_document_content(row)
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df
    
    @staticmethod
    def _extract_type(row: pd.Series) -> str:
        """Extract type display text."""
        type_obj = row.get('resource.type')
        if isinstance(type_obj, dict):
            coding = type_obj.get('coding')
            if isinstance(coding, list) and len(coding) > 0:
                return coding[0].get('display', coding[0].get('code'))
        return None
    
    @staticmethod
    def _extract_category(row: pd.Series) -> str:
        """Extract category display text."""
        categories = row.get('resource.category')
        if isinstance(categories, list) and len(categories) > 0:
            coding = categories[0].get('coding')
            if isinstance(coding, list) and len(coding) > 0:
                return coding[0].get('display', coding[0].get('code'))
        return None
    
    @staticmethod
    def _extract_author(row: pd.Series) -> str:
        """Extract author display name."""
        authors = row.get('resource.author')
        if isinstance(authors, list) and len(authors) > 0:
            return authors[0].get('display')
        return None
    
    @staticmethod
    def _extract_encounter_id(row: pd.Series) -> str:
        """Extract encounter ID from context."""
        encounters = row.get('resource.context.encounter')
        if isinstance(encounters, list) and len(encounters) > 0:
            reference = encounters[0].get('reference')
            if isinstance(reference, str) and '/' in reference:
                return reference.split('/')[-1]
        return None
    
    @staticmethod
    def _extract_document_content(row: pd.Series) -> str:
        """Extract and decode base64-encoded document content."""
        content = row.get('resource.content')
        if isinstance(content, list) and len(content) > 0:
            attachment = content[0].get('attachment')
            if isinstance(attachment, dict):
                data = attachment.get('data')
                if isinstance(data, str):
                    try:
                        # Decode base64
                        decoded_bytes = base64.b64decode(data)
                        # Try to decode as UTF-8
                        decoded_text = decoded_bytes.decode('utf-8', errors='replace')
                        return decoded_text
                    except Exception:
                        return "[Could not decode content]"
        return None


