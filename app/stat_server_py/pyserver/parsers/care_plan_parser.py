"""
Parser for FHIR CarePlan resources.

Extracts clinically relevant fields from CarePlan resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class CarePlanParser(BaseParser):
    """Parser for FHIR CarePlan resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of CarePlan resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR CarePlan resources
            
        Returns:
            DataFrame with clinically relevant CarePlan fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = CarePlanParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            parsed_row['intent'] = row.get('resource.intent')
            
            # Category - extract text from categories (might be multiple)
            categories = row.get('resource.category')
            if isinstance(categories, list):
                category_texts = []
                for cat in categories:
                    if isinstance(cat, dict):
                        text = cat.get('text')
                        if text:
                            category_texts.append(text)
                        else:
                            # Try to get display from coding
                            coding = cat.get('coding')
                            if isinstance(coding, list) and len(coding) > 0:
                                display = coding[0].get('display')
                                if display:
                                    category_texts.append(display)
                parsed_row['category'] = ", ".join(category_texts) if category_texts else None
            else:
                parsed_row['category'] = None
            
            # Encounter reference
            parsed_row['encounter_id'] = CarePlanParser._extract_id_from_reference(
                row.get('resource.encounter.reference')
            )
            
            # Period - try flattened structure first
            period_start = row.get('resource.period.start')
            period_end = row.get('resource.period.end')
            
            if period_start or period_end:
                parsed_row['period_start'] = CarePlanParser._extract_date(period_start)
                parsed_row['period_end'] = CarePlanParser._extract_date(period_end)
            else:
                # Fallback: try as nested dict
                period = row.get('resource.period')
                if isinstance(period, dict):
                    parsed_row['period_start'] = CarePlanParser._extract_date(period.get('start'))
                    parsed_row['period_end'] = CarePlanParser._extract_date(period.get('end'))
                else:
                    parsed_row['period_start'] = None
                    parsed_row['period_end'] = None
            
            # Activities
            activities = row.get('resource.activity')
            if isinstance(activities, list):
                activity_texts = []
                for activity in activities:
                    if isinstance(activity, dict):
                        detail = activity.get('detail')
                        if isinstance(detail, dict):
                            code = detail.get('code')
                            if isinstance(code, dict):
                                text = code.get('text')
                                if text:
                                    activity_texts.append(text)
                parsed_row['activities'] = ", ".join(activity_texts) if activity_texts else None
            else:
                parsed_row['activities'] = None
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

