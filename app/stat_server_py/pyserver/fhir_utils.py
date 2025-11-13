"""
FHIR Resource Utilities Module

This module provides utility functions for fetching and processing FHIR resources
from a HAPI FHIR server. It includes functions for extracting patient details,
resource display names, and aggregating resources by type.
"""

import logging
import requests
import datetime
import io
import numpy as np
import matplotlib.pyplot as plt
from fastapi import HTTPException, Response, Query
from typing import Dict, List, Set, Any, Optional, Tuple
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class FHIRResourceProcessor:
    def __init__(self, hapi_url: str):
        """
        Initialize the FHIR Resource Processor.
        
        Args:
            hapi_url: The base URL of the HAPI FHIR server
        """
        self.hapi_url = hapi_url.rstrip('/')
        
    async def fetch_fhir_resources(self, resource_type: str, include_patients: bool = True, count: int = 1000, cohort_id: str = None) -> Dict:
        """
        Fetch FHIR resources with included patient data.
        
        Args:
            resource_type: The FHIR resource type to fetch (e.g., 'Condition', 'Procedure', 'Observation')
            include_patients: Whether to include patient resources
            count: Maximum number of resources to fetch
            cohort_id: Optional cohort ID to filter resources by cohort tag
            
        Returns:
            dict: The FHIR Bundle response
        """
        try:
            logger.info(f"Fetching {resource_type} resources from HAPI FHIR server")
            
            # Build query parameters
            params = [f"_count={count}"]
            
            # Add cohort tag filter if specified
            if cohort_id:
                params.append(f"_tag=urn:charm:cohort|{cohort_id}")
            
            # Add patient include if needed
            if include_patients:
                params.append(f"_include={resource_type}:patient")
            
            # Construct URL with all parameters
            query_string = "&".join(params)
            url = f"{self.hapi_url}/{resource_type}?{query_string}"
            
            logger.info(f"Making direct FHIR API call to: {url}")
            response = requests.get(url)
            response.raise_for_status()
            
            return response.json()
        except requests.RequestException as e:
            error_msg = f"Error connecting to HAPI FHIR server: {str(e)}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    def extract_patient_details(self, resource: Dict) -> Optional[str]:
        """
        Extract patient details from a FHIR Patient resource and format as a string.
        
        Args:
            resource: The FHIR Patient resource
            
        Returns:
            str: Formatted patient details string with ID, gender, and age
        """
        patient_id = resource.get('id')
        if not patient_id:
            return None
            
        # Extract gender
        gender = resource.get('gender', 'unknown')
        gender_display = gender.capitalize() if gender != 'unknown' else 'Unknown gender'
        
        # Extract birth date and calculate age
        birth_date = resource.get('birthDate', '')
        age_str = 'Unknown age'
        
        if birth_date:
            try:
                # Parse the birth date
                birth_date_obj = datetime.datetime.strptime(birth_date, '%Y-%m-%d').date()
                
                # Calculate age
                today = datetime.date.today()
                age = today.year - birth_date_obj.year
                
                # Adjust age if birthday hasn't occurred yet this year
                if (today.month, today.day) < (birth_date_obj.month, birth_date_obj.day):
                    age -= 1
                    
                age_str = f"{age} years"
            except ValueError:
                # If date format is invalid
                pass
        
        # Format the patient details as a string
        return f"ID: {patient_id}, {gender_display}, {age_str}"

    def extract_display_name(self, resource: Dict, resource_type: str) -> str:
        """
        Extract display name from a FHIR resource.
        
        Args:
            resource: The FHIR resource
            resource_type: The type of resource ('Condition', 'Procedure', 'Observation')
            
        Returns:
            str: The display name of the resource
        """
        default_name = f"Unknown {resource_type}"
        
        # For most resources, the display name is in the code.coding.display or code.text
        if 'code' in resource:
            # First try to get from coding.display
            if 'coding' in resource['code'] and resource['code']['coding']:
                for coding in resource['code']['coding']:
                    if 'display' in coding:
                        return coding['display']
            
            # If not found, try code.text
            if 'text' in resource['code']:
                return resource['code']['text']
        
        # For observations, we might want to include the value
        if resource_type == 'Observation':
            display_name = default_name
            value_summary = ""
            
            # Get the basic display name first
            if 'code' in resource:
                if 'coding' in resource['code'] and resource['code']['coding']:
                    for coding in resource['code']['coding']:
                        if 'display' in coding:
                            display_name = coding['display']
                            break
                elif 'text' in resource['code']:
                    display_name = resource['code']['text']
            
            # Then add the value if available
            if 'valueQuantity' in resource:
                value = resource['valueQuantity'].get('value')
                unit = resource['valueQuantity'].get('unit')
                if value is not None:
                    value_summary = f"{value} {unit if unit else ''}".strip()
            elif 'valueCodeableConcept' in resource:
                if 'coding' in resource['valueCodeableConcept'] and resource['valueCodeableConcept']['coding']:
                    value_summary = resource['valueCodeableConcept']['coding'][0].get('display', '')
            elif 'valueString' in resource:
                value_summary = resource['valueString']
                
            # Combine display name with value summary if available
            if value_summary:
                return f"{display_name}: {value_summary}"
            return display_name
        
        return default_name

    def extract_patient_reference(self, resource: Dict) -> Optional[str]:
        """
        Extract patient reference from a FHIR resource.
        
        Args:
            resource: The FHIR resource
            
        Returns:
            str or None: The patient ID or None if not found
        """
        if 'subject' in resource and 'reference' in resource['subject']:
            patient_ref = resource['subject']['reference']
            if patient_ref.startswith('Patient/'):
                return patient_ref[8:]
        return None

    def extract_codes(self, resource: Dict) -> Set[str]:
        """
        Extract codes from a FHIR resource.
        
        Args:
            resource: The FHIR resource
            
        Returns:
            set: Set of codes
        """
        codes = set()
        if 'code' in resource and 'coding' in resource['code'] and resource['code']['coding']:
            for coding in resource['code']['coding']:
                if 'code' in coding:
                    codes.add(coding['code'])
        return codes

    async def process_fhir_resources(self, resource_type: str, include_patients: bool = True, include_patient_details: bool = True, cohort_id: str = None) -> Dict:
        """
        Process FHIR resources and return a summary.
        
        Args:
            resource_type: The FHIR resource type to process (e.g., 'Condition', 'Procedure', 'Observation')
            include_patients: Whether to include patient IDs
            include_patient_details: Whether to include formatted patient details
            cohort_id: Optional cohort ID to filter resources by cohort tag
            
        Returns:
            dict: Summary of the resources
        """
        try:
            # Fetch resources from HAPI FHIR server
            bundle = await self.fetch_fhir_resources(resource_type, include_patients=include_patients, cohort_id=cohort_id)
            
            # Initialize result structure
            resource_name_plural = resource_type.lower() + 's'
            result = {
                resource_name_plural: [],
                f'total_{resource_name_plural}': 0,
                f'unique_{resource_type.lower()}_types': 0,
                'total_patients': 0
            }
            
            # Check if bundle has entries
            if 'entry' not in bundle or not bundle['entry']:
                logger.warning(f"No entries found in bundle for {resource_type} with cohort_id={cohort_id}")
                return result
                
            # Extract resources and patients
            resources = []
            patients = {}
            patients_by_id = {}  # Will store formatted patient details by ID
            
            for entry in bundle['entry']:
                if 'resource' not in entry:
                    continue
                    
                resource = entry['resource']
                resource_type_actual = resource.get('resourceType', '')
                
                if resource_type_actual == resource_type:
                    resources.append(resource)
                elif resource_type_actual == 'Patient' and include_patients:
                    patient_id = resource.get('id')
                    if patient_id:
                        patients[patient_id] = resource
                        
                        # Format patient details for display
                        if include_patient_details:
                            try:
                                # Extract gender
                                gender = resource.get('gender', 'Unknown')
                                
                                # Extract birth date and calculate age
                                birth_date = resource.get('birthDate')
                                age = None
                                if birth_date:
                                    try:
                                        birth_year = int(birth_date.split('-')[0])
                                        current_year = datetime.now().year
                                        age = current_year - birth_year
                                        
                                        # Adjust age if birthday hasn't occurred yet this year
                                        if len(birth_date.split('-')) >= 3:
                                            birth_month = int(birth_date.split('-')[1])
                                            birth_day = int(birth_date.split('-')[2])
                                            today = datetime.now()
                                            if (today.month, today.day) < (birth_month, birth_day):
                                                age -= 1
                                    except Exception as e:
                                        logger.warning(f"Error calculating age from birthDate '{birth_date}': {str(e)}")
                                
                                # Format the patient detail string
                                age_str = f"Age: {age}y" if age is not None else "Age: Unknown"
                                patients_by_id[patient_id] = f"ID: {patient_id}, {gender.capitalize()}, {age_str}"
                                
                            except Exception as e:
                                logger.warning(f"Error formatting patient details for ID {patient_id}: {str(e)}")
                                patients_by_id[patient_id] = f"ID: {patient_id}, Unknown gender, Unknown age"
            
            logger.info(f"Found {len(resources)} {resource_type} resources and {len(patients)} patient resources")
            
            # Process resources
            resource_counts = {}
            patient_resource_map = {}
            
            for resource in resources:
                # Extract display name
                display_name = self.extract_display_name(resource, resource_type)
                
                # Extract patient reference
                patient_ref = self.extract_patient_reference(resource)
                patient_id = patient_ref.split('/')[-1] if patient_ref else None
                
                # Count resources by type
                if display_name not in resource_counts:
                    resource_counts[display_name] = {
                        f'{resource_type.lower()}_name': display_name,
                        'count': 0,
                        'patients': set()
                    }
                    
                resource_counts[display_name]['count'] += 1
                
                # Track patients with this resource type
                if patient_id:
                    resource_counts[display_name]['patients'].add(patient_id)
                    
                    # Map patients to resources
                    if patient_id not in patient_resource_map:
                        patient_resource_map[patient_id] = set()
                    patient_resource_map[patient_id].add(display_name)
            
            # Convert to list and add patient details if requested
            resource_list = []
            for name, data in resource_counts.items():
                item = {
                    f'{resource_type.lower()}_name': name,
                    'count': data['count']
                }
                
                # Add patient IDs or details
                # Add patient information based on the requested detail level
                if include_patients:
                    # Always include the raw patient IDs for internal use
                    item["patient_ids"] = list(data["patients"])
                    
                    if include_patient_details:
                        # Get patient details for each patient ID
                        patient_details = []
                        for patient_id in data["patients"]:
                            if patient_id in patients_by_id:
                                # Patient details are already formatted as a string
                                patient_details.append(patients_by_id[patient_id])
                            else:
                                # For patients without details, just show the ID
                                patient_details.append(f"ID: {patient_id}, Unknown gender, Unknown age")
                        item["patients"] = patient_details
                
                resource_list.append(item)
            
            # Sort by frequency (most common first)
            resource_list.sort(key=lambda x: x["count"], reverse=True)
            
            # Define resource name variables
            resource_name_singular = resource_type.lower()
            resource_name_plural = f"{resource_name_singular}s"
            
            logger.info(f"Processed {len(resource_list)} {resource_name_plural} with {len(patients_by_id)} patients")
            
            return {
                resource_name_plural: resource_list,
                f"total_{resource_name_plural}": sum(r["count"] for r in resource_list),
                f"unique_{resource_name_singular}_types": len(resource_list),
                "total_patients": len(patients_by_id) if include_patient_details else None
            }
        
        except Exception as e:
            error_msg = f"Error retrieving all {resource_type.lower()}s: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(status_code=500, detail=error_msg)
            
    def _get_image_response(self, plt) -> Response:
        """
        Helper function to convert matplotlib plot to FastAPI response
        
        Args:
            plt: Matplotlib pyplot instance
            
        Returns:
            Response: FastAPI response with PNG image
        """
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        plt.close()  # Close the figure to free memory
        buf.seek(0)
        
        return Response(content=buf.getvalue(), media_type="image/png")
    
    def _prepare_visualization_data(self, resource_data: Dict, resource_type: str, limit: int = 20) -> Tuple[List[str], List[int]]:
        """
        Prepare data for visualization from resource summary
        
        Args:
            resource_data: Resource data from process_fhir_resources
            resource_type: Type of resource ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include
            
        Returns:
            Tuple[List[str], List[int]]: Names and counts for visualization
        """
        resource_name_plural = resource_type.lower() + 's'
        name_field = resource_type.lower() + '_name'
        
        if not resource_data or resource_name_plural not in resource_data or not resource_data[resource_name_plural]:
            return [], []
        
        # Sort resources by count and take the top 'limit'
        resources = sorted(resource_data[resource_name_plural], key=lambda x: x["count"], reverse=True)[:limit]
        
        # Extract names and counts
        names = []
        counts = []
        
        for resource in resources:
            # Use the full name without truncation
            name = resource[name_field]
            names.append(name)
            counts.append(resource["count"])
            
        return names, counts
        
    def _extract_age_from_patient_detail(self, patient_detail: str) -> Optional[int]:
        """
        Extract age from patient detail string
        
        Args:
            patient_detail: String containing patient details in format "ID: <id>, <Gender>, Age: <age>"
            
        Returns:
            int: Age in years, or None if not found
        """
        try:
            # Format is "ID: <id>, <Gender>, Age: <age>"
            parts = patient_detail.split(", ")
            if len(parts) >= 3:
                age_part = parts[2]  # Should be "Age: <age>"
                if "age:" in age_part.lower():
                    age_str = age_part.lower().replace("age:", "").strip()
                    if age_str.endswith("y"):  # Handle "23y" format
                        age_str = age_str[:-1]
                    if age_str.endswith("years"):  # Handle "23 years" format
                        age_str = age_str.replace("years", "").strip()
                    if age_str.isdigit():
                        age = int(age_str)
                        # Log the extracted age for debugging
                        logger.debug(f"Extracted age {age} from patient detail: {patient_detail}")
                        return age
                    else:
                        logger.warning(f"Non-numeric age string: '{age_str}' from patient detail: {patient_detail}")
            else:
                logger.warning(f"Patient detail doesn't have expected format: {patient_detail}")
        except Exception as e:
            logger.error(f"Error extracting age from patient detail: {patient_detail}, error: {str(e)}")
        return None
    
    def _get_age_bracket(self, age: int, bracket_size: int = 5) -> str:
        """
        Get age bracket for a given age
        
        Args:
            age: Age in years
            bracket_size: Size of each age bracket in years
            
        Returns:
            str: Age bracket label (e.g., "0-4", "5-9", etc.)
        """
        if age < 0:
            return "Unknown"
        
        lower_bound = (age // bracket_size) * bracket_size
        upper_bound = lower_bound + bracket_size - 1
        return f"{lower_bound}-{upper_bound}"
    
    def _prepare_gender_visualization_data(self, resource_data: Dict, resource_type: str, limit: int = 10) -> Dict[str, Tuple[List[str], List[int]]]:
        """
        Prepare gender-specific data for visualization from resource summary
        
        Args:
            resource_data: Resource data from process_fhir_resources with patient details
            resource_type: Type of resource ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include per gender
            
        Returns:
            Dict[str, Tuple[List[str], List[int]]]: Gender-specific names and counts for visualization
        """
        resource_name_plural = resource_type.lower() + 's'
        name_field = resource_type.lower() + '_name'
        
        if not resource_data or resource_name_plural not in resource_data or not resource_data[resource_name_plural]:
            return {}
        
        # Create gender-specific data structures
        gender_data = {}
        resources = resource_data[resource_name_plural]
        
        # Process each resource and organize by gender
        for resource in resources:
            if "patients" not in resource:
                continue
                
            # Extract resource name
            name = resource[name_field]
            if len(name) > 40:
                name = name[:37] + "..."
                
            # Group patients by gender
            gender_counts = {}
            for patient_detail in resource["patients"]:
                # Extract gender from patient detail string
                # Format is "ID: <id>, <Gender>, <Age>"
                try:
                    parts = patient_detail.split(", ")
                    if len(parts) >= 2:
                        gender = parts[1].lower()
                        gender_counts[gender] = gender_counts.get(gender, 0) + 1
                except Exception:
                    continue
            
            # Add to gender-specific data
            for gender, count in gender_counts.items():
                if gender not in gender_data:
                    gender_data[gender] = {"names": [], "counts": []}
                
                # Check if this resource is already in the list for this gender
                if name in gender_data[gender]["names"]:
                    idx = gender_data[gender]["names"].index(name)
                    gender_data[gender]["counts"][idx] += count
                else:
                    gender_data[gender]["names"].append(name)
                    gender_data[gender]["counts"].append(count)
        
        # Sort and limit data for each gender
        result = {}
        for gender, data in gender_data.items():
            # Sort by count (descending)
            sorted_indices = sorted(range(len(data["counts"])), key=lambda i: data["counts"][i], reverse=True)
            
            # Take top 'limit' items
            names = [data["names"][i] for i in sorted_indices[:limit]]
            counts = [data["counts"][i] for i in sorted_indices[:limit]]
            
            result[gender] = (names, counts)
            
        return result
        
    def _prepare_age_bracket_visualization_data(self, resource_data: Dict, resource_type: str, limit: int = 10, bracket_size: int = 5) -> Dict[str, Tuple[List[str], List[int]]]:
        """
        Prepare age bracket-specific data for visualization from resource summary
        
        Args:
            resource_data: Resource data from process_fhir_resources with patient details
            resource_type: Type of resource ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include per age bracket
            bracket_size: Size of each age bracket in years
            
        Returns:
            Dict[str, Tuple[List[str], List[int]]]: Age bracket-specific names and counts for visualization
        """
        resource_name_plural = resource_type.lower() + 's'
        name_field = resource_type.lower() + '_name'
        
        if not resource_data:
            logger.warning(f"No resource data provided for {resource_type} visualization")
            return {}
            
        if resource_name_plural not in resource_data:
            logger.warning(f"'{resource_name_plural}' key not found in resource data. Available keys: {list(resource_data.keys())}")
            return {}
            
        if not resource_data[resource_name_plural]:
            logger.warning(f"Empty {resource_name_plural} list in resource data")
            return {}
        
        # Create age bracket-specific data structures
        age_bracket_data = {}
        resources = resource_data[resource_name_plural]
        
        # Debug log
        logger.info(f"Processing {len(resources)} {resource_name_plural} for age bracket visualization")
        
        # Track statistics for debugging
        total_resources = len(resources)
        resources_with_patients = 0
        resources_with_age_data = 0
        total_patients = 0
        patients_with_age = 0
        
        # Process each resource and organize by age bracket
        for resource in resources:
            if "patients" not in resource:
                logger.debug(f"Resource {resource.get(name_field, 'unknown')} has no patients data")
                continue
                
            resources_with_patients += 1
            total_patients += len(resource["patients"])
                
            # Extract resource name
            name = resource[name_field]
            if len(name) > 40:
                name = name[:37] + "..."
                
            # Group patients by age bracket
            age_bracket_counts = {}
            resource_has_age_data = False
            
            for patient_detail in resource["patients"]:
                # Extract age from patient detail string
                age = self._extract_age_from_patient_detail(patient_detail)
                if age is not None:
                    patients_with_age += 1
                    resource_has_age_data = True
                    age_bracket = self._get_age_bracket(age, bracket_size)
                    age_bracket_counts[age_bracket] = age_bracket_counts.get(age_bracket, 0) + 1
            
            if resource_has_age_data:
                resources_with_age_data += 1
            
            # Add to age bracket-specific data
            for age_bracket, count in age_bracket_counts.items():
                if age_bracket not in age_bracket_data:
                    age_bracket_data[age_bracket] = {"names": [], "counts": []}
                
                # Check if this resource is already in the list for this age bracket
                if name in age_bracket_data[age_bracket]["names"]:
                    idx = age_bracket_data[age_bracket]["names"].index(name)
                    age_bracket_data[age_bracket]["counts"][idx] += count
                else:
                    age_bracket_data[age_bracket]["names"].append(name)
                    age_bracket_data[age_bracket]["counts"].append(count)
        
        # Log summary statistics
        logger.info(f"Age bracket visualization stats for {resource_type}:")
        logger.info(f"  Total resources: {total_resources}")
        logger.info(f"  Resources with patients: {resources_with_patients}")
        logger.info(f"  Resources with age data: {resources_with_age_data}")
        logger.info(f"  Total patients: {total_patients}")
        logger.info(f"  Patients with age data: {patients_with_age}")
        logger.info(f"  Age brackets found: {list(age_bracket_data.keys())}")
        
        # If no age data was found, return empty result
        if not age_bracket_data:
            logger.warning(f"No age bracket data could be extracted for {resource_type}")
            return {}
        
        # Sort and limit data for each age bracket
        result = {}
        
        # Sort age brackets naturally
        try:
            sorted_brackets = sorted(age_bracket_data.keys(), 
                                   key=lambda x: int(x.split('-')[0]) if x != "Unknown" else float('inf'))
        except Exception as e:
            logger.error(f"Error sorting age brackets: {str(e)}. Using unsorted brackets.")
            sorted_brackets = list(age_bracket_data.keys())
        
        for age_bracket in sorted_brackets:
            data = age_bracket_data[age_bracket]
            # Sort by count (descending)
            sorted_indices = sorted(range(len(data["counts"])), key=lambda i: data["counts"][i], reverse=True)
            
            # Take top 'limit' items
            names = [data["names"][i] for i in sorted_indices[:limit]]
            counts = [data["counts"][i] for i in sorted_indices[:limit]]
            
            result[age_bracket] = (names, counts)
            
        return result
    
    async def visualize_resource(self, resource_type: str, limit: int = 20, cohort_id: str = None) -> Response:
        """
        Generate a bar chart visualization of the most common resource types.
        
        Args:
            resource_type: Type of resource to visualize ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include
            cohort_id: Optional cohort ID to filter resources by cohort tag
            
        Returns:
            Response: PNG image of the visualization
        """
        try:
            logger.info(f"Generating visualization of {resource_type.lower()}s")
            
            # Get resource data without patient details
            resource_data = await self.process_fhir_resources(resource_type, include_patients=False, cohort_id=cohort_id)
            
            # Prepare data for visualization
            names, counts = self._prepare_visualization_data(resource_data, resource_type, limit)
            
            if not names or not counts:
                # Return a simple image saying no data
                plt.figure(figsize=(10, 6))
                plt.text(0.5, 0.5, f"No {resource_type.lower()} data available", 
                         horizontalalignment='center', verticalalignment='center', fontsize=14)
                plt.axis('off')
                return self._get_image_response(plt)
            
            # Create the visualization with more space for labels
            # Calculate figure width based on the longest label
            max_label_len = max([len(name) for name in names]) if names else 0
            fig_width = max(12, 8 + (max_label_len * 0.1))  # Base width + additional width for long labels
            fig_height = max(6, len(names) * 0.4)  # Adjust height based on number of items
            
            plt.figure(figsize=(fig_width, fig_height))
            
            # Create horizontal bar chart
            y_pos = np.arange(len(names))
            plt.barh(y_pos, counts, align='center', alpha=0.7, color='skyblue')
            plt.yticks(y_pos, names)
            plt.xlabel('Number of Occurrences')
            plt.title(f'Most Common {resource_type} Types')
            
            # Adjust layout to ensure labels are visible
            plt.subplots_adjust(left=0.3)  # Increase left margin for labels
            plt.tight_layout()
            
            # Add count labels to the bars
            for i, v in enumerate(counts):
                plt.text(v + 0.1, i, str(v), color='black', va='center')
            
            # Return the image as a response
            return self._get_image_response(plt)
            
        except Exception as e:
            error_msg = f"Error generating {resource_type.lower()} visualization: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(status_code=500, detail=error_msg)
            
    async def visualize_resource_by_gender(self, resource_type: str, limit: int = 10, cohort_id: str = None) -> Response:
        """
        Generate a visualization of resources broken down by gender
        
        Args:
            resource_type: Type of resource ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include per gender
            cohort_id: Optional cohort ID to filter resources by cohort tag
            
        Returns:
            FastAPI Response with PNG image
        """
        try:
            # Process resources with patient details
            resource_data = await self.process_fhir_resources(
                resource_type, 
                include_patients=True,
                include_patient_details=True,
                cohort_id=cohort_id
            )
            
            # Debug logging
            logger.info(f"Visualizing {resource_type} by age bracket with cohort_id={cohort_id}")
            logger.info(f"Resource data keys: {resource_data.keys() if resource_data else 'None'}")
            resource_name_plural = resource_type.lower() + 's'
            if resource_name_plural in resource_data:
                logger.info(f"Found {len(resource_data[resource_name_plural])} {resource_name_plural}")
            else:
                logger.info(f"No {resource_name_plural} found in resource data")
            
            # Prepare data for visualization by gender
            gender_data = self._prepare_gender_visualization_data(resource_data, resource_type, limit)
            
            if not gender_data:
                return Response(content="No data available for visualization", media_type="text/plain")
            
            # Set up the figure based on number of genders and longest label
            num_genders = len(gender_data)
            
            # Calculate maximum label length across all genders
            max_label_len = 0
            for gender, (names, _) in gender_data.items():
                if names:
                    max_label_len = max(max_label_len, max([len(name) for name in names]))
            
            # Calculate figure dimensions
            fig_width = max(12, 8 + (max_label_len * 0.1))  # Base width + additional width for long labels
            fig_height = max(4, 2 + num_genders * 0.5)  # Base height + additional height per gender
            
            # Create figure with subplots - one row per gender
            fig, axes = plt.subplots(num_genders, 1, figsize=(fig_width, fig_height * num_genders), squeeze=False)
            
            # Color mapping for genders
            color_map = {
                "male": "lightblue",
                "female": "lightpink",
                # Default for any other gender
                "other": "lightgreen"
            }
            
            # Plot data for each gender
            for i, (gender, (names, counts)) in enumerate(gender_data.items()):
                ax = axes[i, 0]
                
                # Get color for this gender
                color = color_map.get(gender.lower(), color_map["other"])
                
                # Create positions for bars
                y_pos = np.arange(len(names))
                
                # Create horizontal bar chart
                ax.barh(y_pos, counts, align='center', alpha=0.7, color=color)
                ax.set_yticks(y_pos)
                ax.set_yticklabels(names)
                ax.invert_yaxis()  # Labels read top-to-bottom
                ax.set_xlabel('Number of Occurrences')
                ax.set_title(f'Most Common {resource_type} Types - {gender.capitalize()}')
                
                # Adjust subplot to ensure labels are visible
                ax.figure.subplots_adjust(left=0.3)  # Increase left margin for labels
                
                # Add count labels to bars
                for j, v in enumerate(counts):
                    ax.text(v + 0.1, j, str(v), color='black', va='center')
            
            plt.tight_layout(pad=3.0)
            
            # Convert plot to PNG image
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close(fig)
            buf.seek(0)
            
            return Response(content=buf.getvalue(), media_type="image/png")
            
        except Exception as e:
            logging.error(f"Error generating visualization by gender for {resource_type}: {str(e)}")
            return Response(
                content=f"Error generating visualization: {str(e)}", 
                media_type="text/plain"
            )
            
    async def visualize_resource_by_age_bracket(self, resource_type: str, limit: int = 10, bracket_size: int = 5, cohort_id: str = None) -> Response:
        """
        Generate a visualization of resources broken down by age brackets
        
        Args:
            resource_type: Type of resource ('Condition', 'Procedure', 'Observation')
            limit: Maximum number of items to include per age bracket
            bracket_size: Size of each age bracket in years
            cohort_id: Optional cohort ID to filter resources by cohort tag
            
        Returns:
            FastAPI Response with PNG image
        """
        try:
            # Process resources with patient details
            resource_data = await self.process_fhir_resources(
                resource_type, 
                include_patients=True,
                include_patient_details=True,
                cohort_id=cohort_id
            )
            
            # Debug logging
            logger.info(f"Visualizing {resource_type} by age bracket with cohort_id={cohort_id}")
            logger.info(f"Resource data keys: {resource_data.keys() if resource_data else 'None'}")
            resource_name_plural = resource_type.lower() + 's'
            if resource_name_plural in resource_data:
                logger.info(f"Found {len(resource_data[resource_name_plural])} {resource_name_plural}")
            else:
                logger.info(f"No {resource_name_plural} found in resource data")
            
            # Prepare data for visualization by age bracket
            try:
                age_bracket_data = self._prepare_age_bracket_visualization_data(
                    resource_data, resource_type, limit, bracket_size
                )
                
                # Debug logging for age bracket data
                logger.info(f"Age bracket data: {list(age_bracket_data.keys()) if age_bracket_data else 'Empty'}")
                
                if not age_bracket_data:
                    logger.warning(f"No age bracket data found for {resource_type} with cohort_id={cohort_id}")
                    # Create a simple text image instead of returning an error
                    fig, ax = plt.subplots(figsize=(10, 6))
                    ax.text(0.5, 0.5, f"No age data available for {resource_type}", 
                            horizontalalignment='center', verticalalignment='center', fontsize=14)
                    ax.axis('off')
                    return self._get_image_response(plt)
            except Exception as e:
                logger.error(f"Error preparing age bracket data: {str(e)}", exc_info=True)
                # Create a simple error image
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.text(0.5, 0.5, f"Error preparing visualization: {str(e)}", 
                        horizontalalignment='center', verticalalignment='center', fontsize=14)
                ax.axis('off')
                return self._get_image_response(plt)
            
            # Set up the figure based on number of age brackets and longest label
            num_brackets = len(age_bracket_data)
            
            # Calculate maximum label length across all age brackets
            max_label_len = 0
            for age_bracket, (names, _) in age_bracket_data.items():
                if names:
                    max_label_len = max(max_label_len, max([len(name) for name in names]))
            
            # Calculate figure dimensions
            fig_width = max(12, 8 + (max_label_len * 0.1))  # Base width + additional width for long labels
            fig_height = max(4, 2 + num_brackets * 0.5)  # Base height + additional height per bracket
            
            # Create figure with subplots - one row per age bracket
            fig, axes = plt.subplots(num_brackets, 1, figsize=(fig_width, fig_height * num_brackets), squeeze=False)
            
            # Generate a color gradient for age brackets
            colors = plt.cm.viridis(np.linspace(0, 0.8, num_brackets))
            
            # Plot data for each age bracket
            for i, (age_bracket, (names, counts)) in enumerate(age_bracket_data.items()):
                ax = axes[i, 0]
                
                # Create positions for bars
                y_pos = np.arange(len(names))
                
                # Create horizontal bar chart
                ax.barh(y_pos, counts, align='center', alpha=0.7, color=colors[i])
                ax.set_yticks(y_pos)
                ax.set_yticklabels(names)
                ax.invert_yaxis()  # Labels read top-to-bottom
                ax.set_xlabel('Number of Occurrences')
                ax.set_title(f'Most Common {resource_type} Types - Age {age_bracket} years')
                
                # Adjust subplot to ensure labels are visible
                ax.figure.subplots_adjust(left=0.3)  # Increase left margin for labels
                
                # Add count labels to bars
                for j, v in enumerate(counts):
                    ax.text(v + 0.1, j, str(v), color='black', va='center')
            
            plt.tight_layout(pad=3.0)
            
            # Convert plot to PNG image
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close(fig)
            buf.seek(0)
            
            return Response(content=buf.getvalue(), media_type="image/png")
            
        except Exception as e:
            logger.error(f"Error generating visualization by age bracket for {resource_type}: {str(e)}", exc_info=True)
            return Response(
                content=f"Error generating visualization: {str(e)}", 
                media_type="text/plain"
            )
