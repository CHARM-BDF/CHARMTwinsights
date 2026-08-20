"""
Parser for FHIR Encounter resources.

Extracts clinically relevant fields from Encounter resources into a clean dataframe format.
This includes hospital stays, doctor visits, and other healthcare encounters.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class EncounterParser(BaseParser):
    """Parser for FHIR Encounter resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Encounter resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Encounter resources
            
        Returns:
            DataFrame with clinically relevant Encounter fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = EncounterParser._extract_cohort(row)
            
            # Basic fields
            parsed_row['status'] = row.get('resource.status')
            
            # Encounter class (e.g., inpatient, outpatient, emergency, ambulatory)
            encounter_class = row.get('resource.class')
            if isinstance(encounter_class, dict):
                parsed_row['encounter_class'] = encounter_class.get('code')
                parsed_row['encounter_class_display'] = encounter_class.get('display')
            elif isinstance(encounter_class, str):
                parsed_row['encounter_class'] = encounter_class
            
            # Encounter type (more specific description)
            type_info = EncounterParser._extract_type(row)
            parsed_row['encounter_type'] = type_info.get('text') or type_info.get('display')
            parsed_row['encounter_type_code'] = type_info.get('code')
            
            # Period (start and end dates)
            parsed_row['start_date'] = EncounterParser._extract_date(
                row.get('resource.period.start')
            )
            parsed_row['end_date'] = EncounterParser._extract_date(
                row.get('resource.period.end')
            )
            
            # Service provider (hospital/clinic name)
            service_provider = row.get('resource.serviceProvider')
            if isinstance(service_provider, dict):
                parsed_row['service_provider'] = service_provider.get('display')
                parsed_row['service_provider_id'] = EncounterParser._extract_id_from_reference(
                    service_provider.get('reference')
                )
            
            # Reason for visit
            reason_info = EncounterParser._extract_reason(row)
            parsed_row['reason'] = reason_info
            
            # Location (where the encounter took place)
            location_info = EncounterParser._extract_location(row)
            parsed_row['location'] = location_info
            
            # Participant (practitioner/doctor)
            participant_info = EncounterParser._extract_participant(row)
            parsed_row['practitioner'] = participant_info
            
            # Hospitalization details (for inpatient stays)
            hospitalization = row.get('resource.hospitalization')
            if isinstance(hospitalization, dict):
                # Admit source
                admit_source = hospitalization.get('admitSource')
                if isinstance(admit_source, dict):
                    parsed_row['admit_source'] = admit_source.get('text') or \
                        EncounterParser._extract_coding_value(admit_source.get('coding'), 'display')
                
                # Discharge disposition
                discharge = hospitalization.get('dischargeDisposition')
                if isinstance(discharge, dict):
                    parsed_row['discharge_disposition'] = discharge.get('text') or \
                        EncounterParser._extract_coding_value(discharge.get('coding'), 'display')
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df
    
    @staticmethod
    def _extract_type(row: pd.Series) -> Dict[str, str]:
        """Extract encounter type information."""
        result = {'text': None, 'code': None, 'display': None}
        
        # Try flattened structure first
        type_list = row.get('resource.type')
        if isinstance(type_list, list) and len(type_list) > 0:
            type_obj = type_list[0]
            if isinstance(type_obj, dict):
                result['text'] = type_obj.get('text')
                coding = type_obj.get('coding')
                if isinstance(coding, list) and len(coding) > 0:
                    code_obj = coding[0]
                    if isinstance(code_obj, dict):
                        result['code'] = code_obj.get('code')
                        result['display'] = code_obj.get('display')
                        if not result['text']:
                            result['text'] = result['display']
        
        return result
    
    @staticmethod
    def _extract_reason(row: pd.Series) -> str:
        """Extract reason for encounter."""
        # Try reasonCode first
        reason_code = row.get('resource.reasonCode')
        if isinstance(reason_code, list) and len(reason_code) > 0:
            reason = reason_code[0]
            if isinstance(reason, dict):
                text = reason.get('text')
                if text:
                    return text
                coding = reason.get('coding')
                if isinstance(coding, list) and len(coding) > 0:
                    code_obj = coding[0]
                    if isinstance(code_obj, dict):
                        return code_obj.get('display')
        
        # Try reasonReference
        reason_ref = row.get('resource.reasonReference')
        if isinstance(reason_ref, list) and len(reason_ref) > 0:
            ref = reason_ref[0]
            if isinstance(ref, dict):
                return ref.get('display')
        
        return None
    
    @staticmethod
    def _extract_location(row: pd.Series) -> str:
        """Extract location information."""
        locations = row.get('resource.location')
        if isinstance(locations, list) and len(locations) > 0:
            loc = locations[0]
            if isinstance(loc, dict):
                location_ref = loc.get('location')
                if isinstance(location_ref, dict):
                    return location_ref.get('display')
        return None
    
    @staticmethod
    def _extract_participant(row: pd.Series) -> str:
        """Extract participant (practitioner) information."""
        participants = row.get('resource.participant')
        if isinstance(participants, list):
            for participant in participants:
                if isinstance(participant, dict):
                    individual = participant.get('individual')
                    if isinstance(individual, dict):
                        display = individual.get('display')
                        if display:
                            return display
        return None
