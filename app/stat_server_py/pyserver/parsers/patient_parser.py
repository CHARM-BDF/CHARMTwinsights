"""
Parser for FHIR Patient resources.

Extracts demographic and identifying information from Patient resources into a clean dataframe format.
"""

import pandas as pd
from typing import Any, Dict, List
from .base_parser import BaseParser


class PatientParser(BaseParser):
    """Parser for FHIR Patient resources."""
    
    @staticmethod
    def parse(df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse a dataframe of Patient resources into a clean format.
        
        Args:
            df: DataFrame with flattened FHIR Patient resources
            
        Returns:
            DataFrame with demographic and identifying Patient fields
        """
        if df.empty:
            return df
        
        parsed_rows = []
        
        for idx, row in df.iterrows():
            parsed_row = {}
            
            # Extract cohort from meta.tag
            parsed_row['cohort'] = PatientParser._extract_cohort(row)
            
            # Name - get official name
            names = row.get('resource.name')
            if isinstance(names, list):
                for name in names:
                    if isinstance(name, dict) and name.get('use') == 'official':
                        given = name.get('given')
                        if isinstance(given, list):
                            parsed_row['given_name'] = ' '.join(given)
                        else:
                            parsed_row['given_name'] = given
                        parsed_row['family_name'] = name.get('family')
                        prefix = name.get('prefix')
                        if isinstance(prefix, list) and len(prefix) > 0:
                            parsed_row['prefix'] = prefix[0]
                        break
            
            # Basic demographics
            parsed_row['gender'] = row.get('resource.gender')
            parsed_row['birth_date'] = row.get('resource.birthDate')
            
            # Deceased
            deceased_datetime = row.get('resource.deceasedDateTime')
            if deceased_datetime:
                parsed_row['deceased_date'] = PatientParser._extract_date(deceased_datetime)
            
            # Race and ethnicity from extensions
            extensions = row.get('resource.extension')
            if isinstance(extensions, list):
                for ext in extensions:
                    if isinstance(ext, dict):
                        url = ext.get('url')
                        
                        # Race
                        if url == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race':
                            sub_extensions = ext.get('extension')
                            if isinstance(sub_extensions, list):
                                for sub_ext in sub_extensions:
                                    if isinstance(sub_ext, dict) and sub_ext.get('url') == 'text':
                                        parsed_row['race'] = sub_ext.get('valueString')
                                        break
                        
                        # Ethnicity
                        elif url == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity':
                            sub_extensions = ext.get('extension')
                            if isinstance(sub_extensions, list):
                                for sub_ext in sub_extensions:
                                    if isinstance(sub_ext, dict) and sub_ext.get('url') == 'text':
                                        parsed_row['ethnicity'] = sub_ext.get('valueString')
                                        break
                        
                        # Birth sex
                        elif url == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex':
                            parsed_row['birth_sex'] = ext.get('valueCode')
                        
                        # Birth place
                        elif url == 'http://hl7.org/fhir/StructureDefinition/patient-birthPlace':
                            birth_address = ext.get('valueAddress')
                            if isinstance(birth_address, dict):
                                city = birth_address.get('city')
                                state = birth_address.get('state')
                                country = birth_address.get('country')
                                parts = [p for p in [city, state, country] if p]
                                parsed_row['birth_place'] = ', '.join(parts) if parts else None
            
            # Address
            addresses = row.get('resource.address')
            if isinstance(addresses, list) and len(addresses) > 0:
                addr = addresses[0]
                if isinstance(addr, dict):
                    line = addr.get('line')
                    if isinstance(line, list) and len(line) > 0:
                        parsed_row['address_line'] = line[0]
                    parsed_row['city'] = addr.get('city')
                    parsed_row['state'] = addr.get('state')
                    parsed_row['postal_code'] = addr.get('postalCode')
            
            # Marital status
            marital_status = row.get('resource.maritalStatus')
            if isinstance(marital_status, dict):
                parsed_row['marital_status'] = marital_status.get('text') or \
                    PatientParser._extract_coding_value(marital_status.get('coding'), field='display')
            
            # Phone
            telecoms = row.get('resource.telecom')
            if isinstance(telecoms, list):
                for telecom in telecoms:
                    if isinstance(telecom, dict) and telecom.get('system') == 'phone':
                        parsed_row['phone'] = telecom.get('value')
                        break
            
            # Language
            communications = row.get('resource.communication')
            if isinstance(communications, list) and len(communications) > 0:
                comm = communications[0]
                if isinstance(comm, dict):
                    language = comm.get('language')
                    if isinstance(language, dict):
                        parsed_row['language'] = language.get('text') or \
                            PatientParser._extract_coding_value(language.get('coding'), field='display')
            
            parsed_rows.append(parsed_row)
        
        result_df = pd.DataFrame(parsed_rows)
        
        # Remove columns that are entirely empty (all None/NaN)
        if not result_df.empty:
            result_df = result_df.dropna(axis=1, how='all')
        
        return result_df

