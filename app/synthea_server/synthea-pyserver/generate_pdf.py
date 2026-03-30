"""
PDF generation module for synthetic patient health summaries.

This module contains functions for:
- Extracting patient information from FHIR data
- Generating PDF documents using ReportLab
"""

import base64
from datetime import datetime
from io import BytesIO
from typing import Dict, List

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

def extract_patient_info_for_pdf(patient_data: dict) -> dict:
    """
    Extract human-readable information from FHIR patient data for PDF generation.
    Filters out technical IDs, codes, and non-interpretable data.
    Separates social findings from medical diagnoses.
    """
    # Keywords to identify social/non-medical findings
    SOCIAL_KEYWORDS = [
        'employment', 'unemployed', 'education', 'educated', 'school', 
        'social isolation', 'housing', 'criminal', 'stress', 'limited social',
        'part-time', 'full-time', 'retired', 'not in labor force'
    ]
    
    result = {
        "personal_info": {},
        "demographics": {},
        "allergies": [],           # Allergies (from clinical notes or AllergyIntolerance)
        "care_team": [],           # Practitioners and care providers
        "insurance": [],           # Insurance/coverage info
        "insurance_history": [],   # Insurance coverage timeline
        "healthcare_costs": {},    # Total costs from ExplanationOfBenefit
        "care_plans": [],          # Active care plans with activities
        "encounters": [],
        "medical_conditions": [],  # Medical diagnoses only
        "social_findings": [],     # Social/lifestyle findings
        "medications": [],
        "medication_administrations": [],  # Actual meds given during hospital stays
        "procedures": [],
        "immunizations": [],
        "imaging_studies": [],     # X-rays, CT scans, etc.
        "vital_signs_history": [], # Full history of vitals
        "observations": [],        # Other observations (labs, etc.)
        "psychosocial_scores": [], # PHQ-2/9, GAD-7, AUDIT-C, DAST-10, Morse Fall Scale
        "medical_devices": [],     # Implantable devices, DME
        "supply_deliveries": [],   # Home monitoring supplies
        "clinical_notes": [],      # Visit summaries / narrative notes
        "clinical_timeline": [],   # Key clinical events with cross-references
        "years_of_data": 0,        # Years of data coverage (calculated from encounters)
    }
    
    # Extract Patient info
    patient = patient_data.get("resources", {}).get("Patient", [{}])[0]
    
    # Personal Info
    names = patient.get("name", [])
    for name in names:
        if name.get("use") == "official":
            given = " ".join(name.get("given", []))
            family = name.get("family", "")
            result["personal_info"]["name"] = f"{given} {family}".strip()
            break
    
    result["personal_info"]["gender"] = patient.get("gender", "Unknown").capitalize()
    result["personal_info"]["birth_date"] = patient.get("birthDate", "Unknown")
    
    # Calculate age
    birth_date = patient.get("birthDate")
    if birth_date:
        try:
            birth = datetime.strptime(birth_date, "%Y-%m-%d")
            today = datetime.now()
            age = today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))
            result["personal_info"]["age"] = f"{age} years"
        except:
            pass
    
    # City/State only
    addresses = patient.get("address", [])
    if addresses:
        addr = addresses[0]
        city = addr.get("city", "")
        state = addr.get("state", "")
        if city or state:
            result["personal_info"]["city/state"] = f"{city}, {state}".strip(", ")
    
    # Marital Status
    ms = patient.get("maritalStatus", {})
    result["personal_info"]["marital_status"] = ms.get("text", "Unknown")
    
    # Demographics from extensions
    for ext in patient.get("extension", []):
        url = ext.get("url", "")
        if "race" in url:
            for sub in ext.get("extension", []):
                if sub.get("url") == "text":
                    result["demographics"]["race"] = sub.get("valueString", "")
        elif "ethnicity" in url:
            for sub in ext.get("extension", []):
                if sub.get("url") == "text":
                    result["demographics"]["ethnicity"] = sub.get("valueString", "")
        elif "birthPlace" in url:
            bp = ext.get("valueAddress", {})
            parts = [bp.get("city"), bp.get("state"), bp.get("country")]
            result["demographics"]["birth_place"] = ", ".join([p for p in parts if p])
    
    # Language
    comms = patient.get("communication", [])
    if comms:
        lang = comms[0].get("language", {})
        result["demographics"]["language"] = lang.get("text", "")
    
    # Extract Care Team (practitioners, organizations)
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "CareTeam":
            participants = r.get("participant", [])
            for p in participants:
                member = p.get("member", {})
                role = p.get("role", [{}])[0].get("text", "")
                member_display = member.get("display", "")
                
                # Skip patient themselves
                if "Patient" in role or not member_display:
                    continue
                
                result["care_team"].append({
                    "name": member_display,
                    "role": role
                })
    
    # Also extract practitioners from Other resources
    practitioners_seen = set()
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "Practitioner":
            prac_id = r.get("id", "")
            if prac_id in practitioners_seen:
                continue
            practitioners_seen.add(prac_id)
            
            name_obj = r.get("name", [{}])[0]
            prefix = name_obj.get("prefix", [""])[0] if name_obj.get("prefix") else ""
            given = " ".join(name_obj.get("given", []))
            family = name_obj.get("family", "")
            full_name = f"{prefix} {given} {family}".strip()
            
            # Only add if not already in care team
            if full_name and not any(ct.get("name") == full_name for ct in result["care_team"]):
                result["care_team"].append({
                    "name": full_name,
                    "role": "Practitioner"
                })
    
    # Extract Insurance/Coverage info
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "ExplanationOfBenefit":
            insurer = r.get("insurer", {}).get("display", "")
            insurance_list = r.get("insurance", [])
            coverage = insurance_list[0].get("coverage", {}).get("display", "") if insurance_list else ""
            
            if insurer and insurer not in [i.get("insurer") for i in result["insurance"]]:
                result["insurance"].append({
                    "insurer": insurer,
                    "coverage": coverage or insurer
                })
    
    # Extract Organizations (healthcare facilities)
    organizations_seen = set()
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "Organization":
            org_name = r.get("name", "")
            if org_name and org_name not in organizations_seen:
                organizations_seen.add(org_name)
                org_type = r.get("type", [{}])[0].get("text", "") if r.get("type") else ""
                address = r.get("address", [{}])[0] if r.get("address") else {}
                addr_str = ""
                if address:
                    city = address.get("city", "")
                    state = address.get("state", "")
                    if city or state:
                        addr_str = f"{city}, {state}".strip(", ")
                
                result["care_team"].append({
                    "name": org_name,
                    "role": org_type or "Healthcare Organization",
                    "location": addr_str
                })
    
    # Build encounter ID to details mapping for linking
    encounter_map = {}
    for enc in patient_data.get("resources", {}).get("Encounter", []):
        enc_id = enc.get("id", "")
        encounter_map[enc_id] = enc
    
    # Extract Encounters (visits/hospital stays) with more details
    for enc in patient_data.get("resources", {}).get("Encounter", []):
        enc_id = enc.get("id", "")
        enc_class = enc.get("class", {})
        enc_type = enc.get("type", [{}])[0].get("text", "")
        period = enc.get("period", {})
        provider = enc.get("serviceProvider", {}).get("display", "")
        
        # Determine encounter type description
        class_code = enc_class.get("code", "") if isinstance(enc_class, dict) else enc_class
        class_map = {"AMB": "Outpatient", "IMP": "Inpatient", "EMER": "Emergency", "HH": "Home Health", "OBSENC": "Observation", "VR": "Virtual"}
        class_display = class_map.get(class_code, class_code)
        
        # Get practitioner info
        practitioner = ""
        participants = enc.get("participant", [])
        for p in participants:
            individual = p.get("individual", {})
            if individual.get("display"):
                practitioner = individual.get("display", "")
                break
        
        # Get reason codes if available
        reason_codes = enc.get("reasonCode", [])
        reasons = [rc.get("text", "") or (rc.get("coding", [{}])[0].get("display", "") if rc.get("coding") else "") for rc in reason_codes]
        reasons = [r for r in reasons if r]
        
        # Get location
        locations = enc.get("location", [])
        location = locations[0].get("location", {}).get("display", "") if locations else ""
        
        # Calculate duration for inpatient stays
        duration = ""
        if period.get("start") and period.get("end"):
            try:
                start_dt = datetime.fromisoformat(period["start"].replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(period["end"].replace("Z", "+00:00"))
                delta = end_dt - start_dt
                days = delta.days
                hours = delta.seconds // 3600
                minutes = (delta.seconds % 3600) // 60
                if days > 0:
                    duration = f"{days}d {hours}h"
                elif hours > 0:
                    duration = f"{hours}h {minutes}m"
                else:
                    duration = f"{minutes}m"
            except:
                pass
        
        result["encounters"].append({
            "id": enc_id,
            "type": enc_type or class_display,
            "class": class_display,
            "start_date": period.get("start", "")[:10] if period.get("start") else "",
            "end_date": period.get("end", "")[:10] if period.get("end") else "",
            "duration": duration,
            "provider": provider,
            "practitioner": practitioner,
            "location": location,
            "reasons": reasons,
        })
    
    # Extract Conditions (diagnoses) - separate medical from social
    for cond in patient_data.get("resources", {}).get("Condition", []):
        code = cond.get("code", {})
        condition_name = code.get("text", "Unknown condition")
        clinical_status = cond.get("clinicalStatus", {}).get("coding", [{}])[0].get("code", "")
        
        # Get encounter reference
        encounter_ref = cond.get("encounter", {}).get("reference", "")
        encounter_id = encounter_ref.split("/")[-1] if encounter_ref else ""
        
        condition_entry = {
            "name": condition_name,
            "status": clinical_status.capitalize(),
            "onset_date": cond.get("onsetDateTime", "")[:10] if cond.get("onsetDateTime") else "",
            "encounter_id": encounter_id
        }
        
        # Check if this is a social finding
        is_social = any(kw.lower() in condition_name.lower() for kw in SOCIAL_KEYWORDS)
        
        if is_social:
            result["social_findings"].append(condition_entry)
        else:
            result["medical_conditions"].append(condition_entry)
    
    # Extract Medications with dosing frequency
    for med in patient_data.get("resources", {}).get("MedicationRequest", []):
        med_code = med.get("medicationCodeableConcept", {})
        dosage_instructions = med.get("dosageInstruction", [])
        dosage = dosage_instructions[0] if dosage_instructions else {}
        
        # Extract dosing frequency
        dosing_text = dosage.get("text", "")
        timing = dosage.get("timing", {})
        repeat = timing.get("repeat", {})
        frequency = repeat.get("frequency", "")
        period = repeat.get("period", "")
        period_unit = repeat.get("periodUnit", "")
        
        # Build frequency string if available
        frequency_str = ""
        if frequency and period and period_unit:
            unit_map = {"d": "daily", "wk": "weekly", "mo": "monthly", "h": "hourly"}
            unit_display = unit_map.get(period_unit, period_unit)
            if frequency == 1 and period == 1:
                frequency_str = f"1× {unit_display}"
            else:
                frequency_str = f"{frequency}× per {period} {unit_display}"
        elif dosing_text:
            frequency_str = dosing_text
        
        # Get encounter reference
        encounter_ref = med.get("encounter", {}).get("reference", "")
        encounter_id = encounter_ref.split("/")[-1] if encounter_ref else ""
        
        result["medications"].append({
            "name": med_code.get("text", "Unknown medication"),
            "status": med.get("status", "").capitalize(),
            "authored_date": med.get("authoredOn", "")[:10] if med.get("authoredOn") else "",
            "dosage": dosing_text,
            "frequency": frequency_str,
            "encounter_id": encounter_id
        })
    
    # Extract Procedures
    for proc in patient_data.get("resources", {}).get("Procedure", []):
        code = proc.get("code", {})
        performed = proc.get("performedPeriod", {}) or proc.get("performedDateTime", "")
        
        start_date = ""
        if isinstance(performed, dict):
            start_date = performed.get("start", "")[:10] if performed.get("start") else ""
        elif isinstance(performed, str):
            start_date = performed[:10]
        
        # Get encounter reference
        encounter_ref = proc.get("encounter", {}).get("reference", "")
        encounter_id = encounter_ref.split("/")[-1] if encounter_ref else ""
        
        result["procedures"].append({
            "name": code.get("text", "Unknown procedure"),
            "status": proc.get("status", "").capitalize(),
            "date": start_date,
            "encounter_id": encounter_id
        })
    
    # Extract Immunizations
    for imm in patient_data.get("resources", {}).get("Immunization", []):
        vaccine = imm.get("vaccineCode", {})
        
        result["immunizations"].append({
            "name": vaccine.get("text", "Unknown vaccine"),
            "date": imm.get("occurrenceDateTime", "")[:10] if imm.get("occurrenceDateTime") else "",
            "status": imm.get("status", "").capitalize()
        })
    
    # Extract Observations - separate vitals from other observations
    vital_codes = ["Body Height", "Body Weight", "Body Mass Index", "Body mass index", "BMI",
                   "Blood Pressure", "Blood pressure", "Heart rate", "Respiratory rate", 
                   "Body temperature", "Pain severity", "Systolic", "Diastolic"]
    
    for obs in patient_data.get("resources", {}).get("Observation", []):
        code = obs.get("code", {})
        code_text = code.get("text", "")
        
        # Get value
        value_str = ""
        if obs.get("valueQuantity"):
            vq = obs["valueQuantity"]
            value_str = f"{vq.get('value', '')} {vq.get('unit', '')}".strip()
        elif obs.get("valueString"):
            value_str = obs["valueString"]
        elif obs.get("valueCodeableConcept"):
            value_str = obs["valueCodeableConcept"].get("text", "")
        
        # Handle component observations (like blood pressure with systolic/diastolic)
        components = obs.get("component", [])
        component_values = []
        for comp in components:
            comp_code = comp.get("code", {}).get("text", "")
            comp_value = ""
            if comp.get("valueQuantity"):
                vq = comp["valueQuantity"]
                comp_value = f"{vq.get('value', '')} {vq.get('unit', '')}".strip()
            if comp_code and comp_value:
                component_values.append(f"{comp_code}: {comp_value}")
        
        if component_values:
            value_str = "; ".join(component_values)
        
        # Get category
        category = ""
        cat_list = obs.get("category", [])
        if cat_list:
            cat_coding = cat_list[0].get("coding", [{}])[0]
            category = cat_coding.get("display", "") or cat_coding.get("code", "")
        
        # Only include observations with meaningful values
        if code_text and value_str:
            obs_entry = {
                "name": code_text,
                "value": value_str,
                "date": obs.get("effectiveDateTime", "")[:10] if obs.get("effectiveDateTime") else "",
                "category": category,
            }
            
            # Check if this is a vital sign
            is_vital = any(v.lower() in code_text.lower() for v in vital_codes)
            
            # Check if this is a psychosocial score
            psych_keywords = ['PHQ', 'GAD', 'AUDIT', 'DAST', 'Morse', 'fall risk', 'depression', 'anxiety']
            is_psych = any(k.lower() in code_text.lower() for k in psych_keywords)
            
            if is_vital:
                result["vital_signs_history"].append(obs_entry)
            elif is_psych:
                result["psychosocial_scores"].append(obs_entry)
            else:
                result["observations"].append(obs_entry)
    
    # Extract Medical Devices & DME from Other resources
    for resource in patient_data.get("resources", {}).get("Other", []):
        if resource.get("resourceType") == "Device":
            device_name = ""
            device_type = resource.get("type", {})
            if device_type:
                device_name = device_type.get("text", "") or (device_type.get("coding", [{}])[0].get("display", "") if device_type.get("coding") else "")
            
            # Get device names from deviceName array
            device_names = resource.get("deviceName", [])
            if device_names:
                device_name = device_names[0].get("name", device_name)
            
            result["medical_devices"].append({
                "name": device_name or "Unknown device",
                "status": resource.get("status", ""),
                "manufacture_date": resource.get("manufactureDate", "")[:10] if resource.get("manufactureDate") else "",
                "expiration_date": resource.get("expirationDate", "")[:10] if resource.get("expirationDate") else "",
                "serial_number": resource.get("serialNumber", ""),
            })
    
    # Extract Supply Deliveries / Home Monitoring
    for resource in patient_data.get("resources", {}).get("Other", []):
        if resource.get("resourceType") == "SupplyDelivery":
            supplied_item = resource.get("suppliedItem", {})
            item_name = ""
            item_concept = supplied_item.get("itemCodeableConcept", {})
            if item_concept:
                item_name = item_concept.get("text", "") or (item_concept.get("coding", [{}])[0].get("display", "") if item_concept.get("coding") else "")
            
            quantity = supplied_item.get("quantity", {}).get("value", "")
            
            result["supply_deliveries"].append({
                "item": item_name or "Unknown supply",
                "quantity": quantity,
                "date": resource.get("occurrenceDateTime", "")[:10] if resource.get("occurrenceDateTime") else "",
                "status": resource.get("status", ""),
            })
    
    # Extract Insurance History from ExplanationOfBenefit
    insurance_periods = {}  # Track coverage by insurer name
    for resource in patient_data.get("resources", {}).get("Other", []):
        if resource.get("resourceType") == "ExplanationOfBenefit":
            # Get coverage info from contained resources
            contained = resource.get("contained", [])
            for c in contained:
                if c.get("resourceType") == "Coverage":
                    coverage_type = c.get("type", {}).get("text", "")
                    payors = c.get("payor", [])
                    payor_name = payors[0].get("display", "") if payors else coverage_type
                    
                    # Get date from the EOB
                    billable_period = resource.get("billablePeriod", {})
                    start_date = billable_period.get("start", "")[:10] if billable_period.get("start") else ""
                    
                    if payor_name and start_date:
                        if payor_name not in insurance_periods:
                            insurance_periods[payor_name] = {"start": start_date, "end": start_date}
                        else:
                            if start_date < insurance_periods[payor_name]["start"]:
                                insurance_periods[payor_name]["start"] = start_date
                            if start_date > insurance_periods[payor_name]["end"]:
                                insurance_periods[payor_name]["end"] = start_date
    
    # Convert to sorted list
    for insurer, period in insurance_periods.items():
        result["insurance_history"].append({
            "insurer": insurer,
            "start_date": period["start"],
            "end_date": period["end"],
        })
    result["insurance_history"].sort(key=lambda x: x.get("start_date", ""))
    
    # Extract Clinical Notes from DocumentReference
    import base64
    for doc in patient_data.get("resources", {}).get("DocumentReference", []):
        doc_type = doc.get("type", {})
        type_text = ""
        for coding in doc_type.get("coding", []):
            if coding.get("display"):
                type_text = coding.get("display")
                break
        
        # Get the note content
        content_list = doc.get("content", [])
        note_text = ""
        for content in content_list:
            attachment = content.get("attachment", {})
            if attachment.get("data"):
                try:
                    decoded = base64.b64decode(attachment["data"]).decode("utf-8")
                    note_text = decoded
                    break
                except:
                    pass
        
        # Get author
        authors = doc.get("author", [])
        author = authors[0].get("display", "") if authors else ""
        
        # Get date
        doc_date = doc.get("date", "")[:10] if doc.get("date") else ""
        
        if note_text:
            result["clinical_notes"].append({
                "type": type_text or "Clinical Note",
                "date": doc_date,
                "author": author,
                "content": note_text,
            })
    
    # Sort clinical notes by date (most recent first) and keep only recent 5
    result["clinical_notes"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["clinical_notes"] = result["clinical_notes"][:5]
    
    # Extract allergies from clinical notes
    allergy_found = False
    for note in result["clinical_notes"]:
        content = note.get("content", "").lower()
        if "no known allergies" in content or "nka" in content:
            result["allergies"] = [{"status": "No Known Allergies"}]
            allergy_found = True
            break
        elif "allergies" in content:
            # Try to extract allergy info
            lines = note.get("content", "").split("\n")
            for i, line in enumerate(lines):
                if "allergies" in line.lower() and i + 1 < len(lines):
                    allergy_line = lines[i + 1].strip()
                    if allergy_line and "no known" not in allergy_line.lower():
                        result["allergies"].append({"name": allergy_line})
                        allergy_found = True
    
    if not allergy_found:
        result["allergies"] = [{"status": "Not documented"}]
    
    # Sort all lists by date (most recent first)
    result["vital_signs_history"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["encounters"].sort(key=lambda x: x.get("start_date", ""), reverse=True)
    result["medical_conditions"].sort(key=lambda x: x.get("onset_date", ""), reverse=True)
    result["social_findings"].sort(key=lambda x: x.get("onset_date", ""), reverse=True)
    result["medications"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["medication_administrations"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["procedures"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["immunizations"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["imaging_studies"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["observations"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["psychosocial_scores"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["supply_deliveries"].sort(key=lambda x: x.get("date", ""), reverse=True)
    result["medical_devices"].sort(key=lambda x: x.get("manufacture_date", ""), reverse=True)
    
    # Calculate years of data from encounters
    if result["encounters"]:
        encounter_dates = [e.get("start_date", "") for e in result["encounters"] if e.get("start_date")]
        if encounter_dates:
            try:
                dates = [datetime.fromisoformat(d.replace("Z", "+00:00")) if "T" in d else datetime.strptime(d, "%Y-%m-%d") for d in encounter_dates]
                earliest = min(dates)
                latest = max(dates)
                years_diff = (latest - earliest).days / 365.25
                result["years_of_data"] = max(1, round(years_diff))  # At least 1 year
            except:
                result["years_of_data"] = 0
    
    # Build encounter lookup for cross-references
    encounter_lookup = {}
    for enc in result["encounters"]:
        enc_id = enc.get("id", "")
        if enc_id:
            encounter_lookup[enc_id] = {
                "type": enc.get("type", ""),
                "date": enc.get("start_date", ""),
                "class": enc.get("class", "")
            }
    
    # Extract Care Plans
    for cp in patient_data.get("resources", {}).get("CarePlan", []):
        status = cp.get("status", "")
        category = cp.get("category", [{}])
        cat_text = ""
        for cat in category:
            t = cat.get("text", "") or (cat.get("coding", [{}])[0].get("display", "") if cat.get("coding") else "")
            if t:
                cat_text = t
                break
        
        period = cp.get("period", {})
        start = period.get("start", "")[:10] if period.get("start") else ""
        end = period.get("end", "")[:10] if period.get("end") else "ongoing"
        
        # Get activities
        activities = []
        for act in cp.get("activity", []):
            detail = act.get("detail", {})
            act_code = detail.get("code", {}).get("text", "")
            act_status = detail.get("status", "")
            if act_code:
                activities.append({"name": act_code, "status": act_status})
        
        # Get conditions this plan addresses
        addresses = []
        for addr in cp.get("addresses", []):
            ref = addr.get("reference", "")
            # Try to find the condition name
            cond_id = ref.split("/")[-1] if ref else ""
            for cond in result["medical_conditions"] + result["social_findings"]:
                if cond.get("encounter_id") == cond_id or ref in str(cond):
                    addresses.append(cond.get("name", ref))
                    break
        
        if cat_text or activities:
            result["care_plans"].append({
                "name": cat_text or "Care Plan",
                "status": status,
                "period": f"{start} to {end}",
                "activities": activities,
                "addresses": addresses
            })
    
    # Extract Healthcare Costs from ExplanationOfBenefit
    total_cost = 0.0
    cost_by_type = {}
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "ExplanationOfBenefit":
            totals = r.get("total", [])
            for t in totals:
                cat = t.get("category", {}).get("coding", [{}])[0].get("code", "")
                if cat == "submitted":
                    amount = t.get("amount", {}).get("value", 0)
                    total_cost += amount
                    
                    # Get service type
                    item = r.get("item", [{}])[0] if r.get("item") else {}
                    service = item.get("productOrService", {}).get("text", "Other")
                    if service:
                        service_type = service[:50]
                        cost_by_type[service_type] = cost_by_type.get(service_type, 0) + amount
    
    result["healthcare_costs"] = {
        "total": total_cost,
        "by_type": sorted(cost_by_type.items(), key=lambda x: x[1], reverse=True)[:10]
    }
    
    # Extract Medication Administrations (actual meds given during hospital stays)
    for ma in patient_data.get("resources", {}).get("MedicationAdministration", []):
        med = ma.get("medicationCodeableConcept", {})
        med_name = med.get("text", "Unknown medication")
        
        # Get encounter info
        enc_ref = ma.get("context", {}).get("reference", "")
        enc_id = enc_ref.split("/")[-1] if enc_ref else ""
        enc_info = encounter_lookup.get(enc_id, {})
        
        dosage = ma.get("dosage", {})
        dose_val = dosage.get("dose", {})
        dose_str = f"{dose_val.get('value', '')} {dose_val.get('unit', '')}".strip() if dose_val else ""
        route = dosage.get("route", {}).get("text", "")
        
        result["medication_administrations"].append({
            "name": med_name,
            "status": ma.get("status", ""),
            "date": ma.get("effectiveDateTime", "")[:10] if ma.get("effectiveDateTime") else "",
            "dose": dose_str,
            "route": route,
            "encounter_type": enc_info.get("type", ""),
            "encounter_date": enc_info.get("date", "")
        })
    
    # Extract Imaging Studies
    for r in patient_data.get("resources", {}).get("Other", []):
        if r.get("resourceType") == "ImagingStudy":
            # Get encounter info
            enc_ref = r.get("encounter", {}).get("reference", "")
            enc_id = enc_ref.split("/")[-1] if enc_ref else ""
            enc_info = encounter_lookup.get(enc_id, {})
            
            # Get series info
            series = r.get("series", [{}])[0] if r.get("series") else {}
            body_site = series.get("bodySite", {}).get("display", "")
            modality = series.get("modality", {}).get("display", "") or (r.get("modality", [{}])[0].get("display", "") if r.get("modality") else "")
            
            result["imaging_studies"].append({
                "description": r.get("description", "") or modality or "Imaging Study",
                "modality": modality,
                "body_site": body_site,
                "date": r.get("started", "")[:10] if r.get("started") else "",
                "encounter_type": enc_info.get("type", ""),
            })
    
    # Add prescriber info to medications (update existing entries)
    for i, med in enumerate(result["medications"]):
        # Find the original medication request to get requester
        for mr in patient_data.get("resources", {}).get("MedicationRequest", []):
            mr_name = mr.get("medicationCodeableConcept", {}).get("text", "")
            if mr_name == med.get("name"):
                requester = mr.get("requester", {}).get("display", "")
                if requester:
                    result["medications"][i]["prescriber"] = requester
                
                # Also get reason if available
                reasons = mr.get("reasonCode", [])
                if reasons:
                    reason_text = reasons[0].get("text", "") or (reasons[0].get("coding", [{}])[0].get("display", "") if reasons[0].get("coding") else "")
                    result["medications"][i]["reason"] = reason_text
                break
    
    # Add encounter context to conditions
    for i, cond in enumerate(result["medical_conditions"]):
        enc_id = cond.get("encounter_id", "")
        if enc_id and enc_id in encounter_lookup:
            enc_info = encounter_lookup[enc_id]
            result["medical_conditions"][i]["encounter_type"] = enc_info.get("type", "")
            result["medical_conditions"][i]["encounter_date"] = enc_info.get("date", "")
    
    # Add encounter context to procedures
    for i, proc in enumerate(result["procedures"]):
        enc_id = proc.get("encounter_id", "")
        if enc_id and enc_id in encounter_lookup:
            enc_info = encounter_lookup[enc_id]
            result["procedures"][i]["encounter_type"] = enc_info.get("type", "")
            result["procedures"][i]["encounter_date"] = enc_info.get("date", "")
    
    # Build Clinical Timeline - key events with cross-references
    timeline_events = []
    
    # Add significant encounters (hospitalizations, emergencies)
    for enc in result["encounters"]:
        enc_class = enc.get("class", "")
        if enc_class in ["Inpatient", "Emergency", "IMP", "EMER"]:
            # Find related conditions diagnosed during this encounter
            related_conditions = []
            for cond in result["medical_conditions"]:
                if cond.get("encounter_id") == enc.get("id"):
                    related_conditions.append(cond.get("name", ""))
            
            # Find related procedures
            related_procedures = []
            for proc in result["procedures"]:
                if proc.get("encounter_id") == enc.get("id"):
                    related_procedures.append(proc.get("name", ""))
            
            timeline_events.append({
                "date": enc.get("start_date", ""),
                "event_type": "Hospital/Emergency Visit",
                "description": enc.get("type", ""),
                "details": {
                    "class": enc_class,
                    "provider": enc.get("provider", ""),
                    "conditions_diagnosed": related_conditions[:3],
                    "procedures_performed": related_procedures[:3]
                }
            })
    
    # Add care plan starts
    for cp in result["care_plans"]:
        if cp.get("status") == "active":
            period = cp.get("period", "")
            start_date = period.split(" to ")[0] if " to " in period else ""
            timeline_events.append({
                "date": start_date,
                "event_type": "Care Plan Started",
                "description": cp.get("name", ""),
                "details": {
                    "activities": [a.get("name", "") for a in cp.get("activities", [])[:3]]
                }
            })
    
    # Add imaging studies
    for img in result["imaging_studies"]:
        timeline_events.append({
            "date": img.get("date", ""),
            "event_type": "Imaging Study",
            "description": f"{img.get('modality', 'Study')} - {img.get('body_site', '')}".strip(" -"),
            "details": {}
        })
    
    # Sort timeline by date
    timeline_events.sort(key=lambda x: x.get("date", ""), reverse=True)
    result["clinical_timeline"] = timeline_events[:20]  # Top 20 most recent significant events
    
    return result


def generate_patient_pdf(patient_info: dict, patient_id: str) -> bytes:
    """
    Generate a PDF document from extracted patient information.
    Shows complete history for all sections (no entry limits).
    Uses Paragraph objects in table cells for text wrapping.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, spaceAfter=12)
    section_style = ParagraphStyle('Section', parent=styles['Heading2'], fontSize=14, spaceBefore=12, spaceAfter=6, textColor=colors.darkblue)
    
    # Cell styles for text wrapping
    cell_style = ParagraphStyle('Cell', parent=styles['Normal'], fontSize=8, leading=10)
    cell_style_9 = ParagraphStyle('Cell9', parent=styles['Normal'], fontSize=9, leading=11)
    header_cell_style = ParagraphStyle('HeaderCell', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')
    
    def P(text, style=cell_style):
        """Helper to create Paragraph for table cell with text wrapping"""
        return Paragraph(str(text) if text else "", style)
    
    story = []
    
    # Title
    patient_name = patient_info.get("personal_info", {}).get("name", f"Patient {patient_id}")
    story.append(Paragraph(f"Synthetic Patient Health Summary", title_style))
    story.append(Paragraph(f"<b>{patient_name}</b> (ID: {patient_id})", styles['Heading2']))
    story.append(Spacer(1, 8))
    
    # Synthetic data notice
    notice_style = ParagraphStyle('Notice', parent=styles['Normal'], fontSize=8, textColor=colors.grey, leading=10)
    story.append(Paragraph("<i>This is a synthetic patient generated using the CHARM-Twinsight tool, based on the open-source Synthea patient generator.</i>", notice_style))
    story.append(Paragraph("<i>Project: https://github.com/CHARM-BDF/CHARMTwinsights</i>", notice_style))
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"<i>To regenerate this PDF: GET http://localhost:8003/patient/{patient_id}/pdf</i>", notice_style))
    story.append(Paragraph(f"<i>To obtain the full FHIR data for this synthetic patient: GET http://localhost:8003/patient/{patient_id}/fhir (or use HAPI FHIR: /Patient/{patient_id}/$everything)</i>", notice_style))
    story.append(Spacer(1, 12))
    
    # ===== SECTION 1: Personal Information =====
    story.append(Paragraph("Personal Information", section_style))
    personal = patient_info.get("personal_info", {})
    personal_data = [
        ["Gender:", personal.get("gender", "N/A")],
        ["Date of Birth:", personal.get("birth_date", "N/A")],
        ["Age:", personal.get("age", "N/A")],
        ["City/State:", personal.get("city/state", "N/A")],
        ["Marital Status:", personal.get("marital_status", "N/A")],
    ]
    t = Table(personal_data, colWidths=[1.5*inch, 5*inch])
    t.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(t)
    story.append(Spacer(1, 12))
    
    # ===== SECTION 1b: Allergies (prominent placement) =====
    allergies = patient_info.get("allergies", [])
    allergy_text = ""
    if allergies:
        if allergies[0].get("status"):
            allergy_text = allergies[0]["status"]
        else:
            allergy_text = ", ".join([a.get("name", "") for a in allergies if a.get("name")])
    if not allergy_text:
        allergy_text = "Not documented"
    
    allergy_style = ParagraphStyle('Allergy', parent=styles['Normal'], fontSize=10, textColor=colors.red if "No Known" not in allergy_text and "Not documented" not in allergy_text else colors.darkgreen)
    story.append(Paragraph(f"<b>Allergies:</b> {allergy_text}", allergy_style))
    story.append(Spacer(1, 12))
    
    # ===== SECTION 2: Demographics =====
    demo = patient_info.get("demographics", {})
    if any(demo.values()):
        story.append(Paragraph("Demographics", section_style))
        demo_data = []
        if demo.get("race"): demo_data.append(["Race:", demo["race"]])
        if demo.get("ethnicity"): demo_data.append(["Ethnicity:", demo["ethnicity"]])
        if demo.get("birth_place"): demo_data.append(["Birth Place:", demo["birth_place"]])
        if demo.get("language"): demo_data.append(["Language:", demo["language"]])
        if demo_data:
            t = Table(demo_data, colWidths=[1.5*inch, 5*inch])
            t.setStyle(TableStyle([('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold')]))
            story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 3: Vital Signs History =====
    vitals = patient_info.get("vital_signs_history", [])
    if vitals:
        story.append(Paragraph(f"Vital Signs History ({len(vitals)} measurements)", section_style))
        
        vital_data = [[P("Measurement", header_cell_style), P("Value", header_cell_style), P("Date", header_cell_style)]]
        for v in vitals:
            vital_data.append([P(v.get("name", ""), cell_style), P(v.get("value", ""), cell_style), P(v.get("date", ""), cell_style)])
        
        t = Table(vital_data, colWidths=[2.5*inch, 2.5*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 6: Immunizations (FULL HISTORY) =====
    immunizations = patient_info.get("immunizations", [])
    if immunizations:
        story.append(Paragraph(f"Immunizations ({len(immunizations)} total)", section_style))
        imm_data = [[P("Vaccine", header_cell_style), P("Date", header_cell_style), P("Status", header_cell_style)]]
        for i in immunizations:
            imm_data.append([P(i.get("name", ""), cell_style_9), P(i.get("date", ""), cell_style_9), P(i.get("status", ""), cell_style_9)])
        
        t = Table(imm_data, colWidths=[4*inch, 1*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightcoral),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 9: Medical Conditions/Diagnoses (with encounter context) =====
    conditions = patient_info.get("medical_conditions", [])
    if conditions:
        story.append(Paragraph(f"Medical Diagnoses ({len(conditions)} total)", section_style))
        # Show active conditions first
        active = [c for c in conditions if c.get("status", "").lower() == "active"]
        resolved = [c for c in conditions if c.get("status", "").lower() != "active"]
        
        cond_data = [[P("Condition", header_cell_style), P("Status", header_cell_style), P("Onset", header_cell_style), P("Diagnosed During", header_cell_style)]]
        for c in (active + resolved):
            encounter_ctx = c.get("encounter_type", "")
            cond_data.append([
                P(c.get("name", ""), cell_style),
                P(c.get("status", ""), cell_style),
                P(c.get("onset_date", ""), cell_style),
                P(encounter_ctx, cell_style)
            ])
        
        t = Table(cond_data, colWidths=[2.5*inch, 0.7*inch, 0.8*inch, 2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 8: Social & Lifestyle Findings (FULL HISTORY with wrapping) =====
    social = patient_info.get("social_findings", [])
    if social:
        story.append(Paragraph(f"Social & Lifestyle Findings ({len(social)} total)", section_style))
        # Show active first
        active = [c for c in social if c.get("status", "").lower() == "active"]
        resolved = [c for c in social if c.get("status", "").lower() != "active"]
        
        social_data = [[P("Finding", header_cell_style), P("Status", header_cell_style), P("Date", header_cell_style)]]
        for c in (active + resolved):
            social_data.append([P(c.get("name", ""), cell_style_9), P(c.get("status", ""), cell_style_9), P(c.get("onset_date", ""), cell_style_9)])
        
        t = Table(social_data, colWidths=[3.5*inch, 1*inch, 1.5*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.beige),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 11: Medications (with dosing frequency) =====
    medications = patient_info.get("medications", [])
    if medications:
        story.append(Paragraph(f"Medications ({len(medications)} total)", section_style))
        active_meds = [m for m in medications if m.get("status", "").lower() == "active"]
        other_meds = [m for m in medications if m.get("status", "").lower() != "active"]
        
        med_data = [[P("Medication", header_cell_style), P("Status", header_cell_style), P("Frequency", header_cell_style), P("Date", header_cell_style)]]
        for m in (active_meds + other_meds):
            med_data.append([
                P(m.get("name", ""), cell_style),
                P(m.get("status", ""), cell_style),
                P(m.get("frequency", ""), cell_style),
                P(m.get("authored_date", ""), cell_style)
            ])
        
        t = Table(med_data, colWidths=[2.8*inch, 0.7*inch, 1.5*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 12: Procedures (with encounter context) =====
    procedures = patient_info.get("procedures", [])
    if procedures:
        story.append(Paragraph(f"Procedures ({len(procedures)} total)", section_style))
        proc_data = [[P("Procedure", header_cell_style), P("Status", header_cell_style), P("Date", header_cell_style), P("During Visit", header_cell_style)]]
        for p in procedures:
            proc_data.append([
                P(p.get("name", ""), cell_style),
                P(p.get("status", ""), cell_style),
                P(p.get("date", ""), cell_style),
                P(p.get("encounter_type", ""), cell_style)
            ])
        
        t = Table(proc_data, colWidths=[2.4*inch, 0.8*inch, 0.8*inch, 2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightyellow),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 11: Healthcare Visits (FULL HISTORY with wrapping) =====
    encounters = patient_info.get("encounters", [])
    if encounters:
        story.append(Paragraph(f"Healthcare Visits ({len(encounters)} total)", section_style))
        enc_data = [[P("Visit Type", header_cell_style), P("Class", header_cell_style), P("Date", header_cell_style), P("Duration", header_cell_style), P("Provider/Location", header_cell_style)]]
        for e in encounters:
            # Use location if available, otherwise provider
            location = e.get("location", "") or e.get("provider", "")
            enc_data.append([
                P(e.get("type", ""), cell_style),
                P(e.get("class", ""), cell_style),
                P(e.get("start_date", ""), cell_style),
                P(e.get("duration", ""), cell_style),
                P(location, cell_style)
            ])
        
        t = Table(enc_data, colWidths=[1.8*inch, 0.7*inch, 0.8*inch, 0.7*inch, 2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lavender),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 14: Medication Administrations (hospital meds) =====
    med_admins = patient_info.get("medication_administrations", [])
    if med_admins:
        story.append(Paragraph(f"Medication Administrations ({len(med_admins)} total)", section_style))
        story.append(Paragraph("<i>Medications actually given during hospital stays</i>", styles['Normal']))
        ma_data = [[P("Medication", header_cell_style), P("Date", header_cell_style), P("Dose", header_cell_style), P("During Visit", header_cell_style)]]
        for ma in med_admins:
            ma_data.append([
                P(ma.get("name", ""), cell_style),
                P(ma.get("date", ""), cell_style),
                P(ma.get("dose", ""), cell_style),
                P(ma.get("encounter_type", ""), cell_style)
            ])
        
        t = Table(ma_data, colWidths=[2.5*inch, 0.8*inch, 1*inch, 1.7*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.palegreen),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 15: Imaging Studies =====
    imaging = patient_info.get("imaging_studies", [])
    if imaging:
        story.append(Paragraph(f"Imaging Studies ({len(imaging)} total)", section_style))
        img_data = [[P("Study", header_cell_style), P("Body Site", header_cell_style), P("Date", header_cell_style), P("During Visit", header_cell_style)]]
        for img in imaging:
            img_data.append([
                P(img.get("description", ""), cell_style),
                P(img.get("body_site", ""), cell_style),
                P(img.get("date", ""), cell_style),
                P(img.get("encounter_type", ""), cell_style)
            ])
        
        t = Table(img_data, colWidths=[2*inch, 2*inch, 0.8*inch, 1.2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightsteelblue),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 16: Psychosocial Scores =====
    psych_scores = patient_info.get("psychosocial_scores", [])
    if psych_scores:
        story.append(Paragraph(f"Psychosocial Assessments ({len(psych_scores)} total)", section_style))
        psych_data = [[P("Assessment", header_cell_style), P("Score", header_cell_style), P("Date", header_cell_style)]]
        for p in psych_scores:
            psych_data.append([
                P(p.get("name", ""), cell_style),
                P(p.get("value", ""), cell_style),
                P(p.get("date", ""), cell_style)
            ])
        
        t = Table(psych_data, colWidths=[3.5*inch, 1.5*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightyellow),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 17: Medical Devices & DME =====
    devices = patient_info.get("medical_devices", [])
    if devices:
        story.append(Paragraph(f"Medical Devices & DME ({len(devices)} total)", section_style))
        dev_data = [[P("Device", header_cell_style), P("Status", header_cell_style), P("Mfg Date", header_cell_style), P("Exp Date", header_cell_style)]]
        for d in devices:
            dev_data.append([
                P(d.get("name", ""), cell_style),
                P(d.get("status", ""), cell_style),
                P(d.get("manufacture_date", ""), cell_style),
                P(d.get("expiration_date", ""), cell_style)
            ])
        
        t = Table(dev_data, colWidths=[3*inch, 1*inch, 1*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightcyan),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 18: Supply Deliveries / Home Monitoring =====
    supplies = patient_info.get("supply_deliveries", [])
    if supplies:
        story.append(Paragraph(f"Supply Deliveries / Home Monitoring ({len(supplies)} total)", section_style))
        sup_data = [[P("Item", header_cell_style), P("Quantity", header_cell_style), P("Date", header_cell_style), P("Status", header_cell_style)]]
        for s in supplies:
            sup_data.append([
                P(s.get("item", ""), cell_style),
                P(str(s.get("quantity", "")), cell_style),
                P(s.get("date", ""), cell_style),
                P(s.get("status", ""), cell_style)
            ])
        
        t = Table(sup_data, colWidths=[3*inch, 1*inch, 1*inch, 1*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightcyan),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 19: Insurance History Timeline =====
    insurance_hist = patient_info.get("insurance_history", [])
    if insurance_hist:
        story.append(Paragraph(f"Insurance Coverage History ({len(insurance_hist)} periods)", section_style))
        ins_data = [[P("Insurer", header_cell_style), P("Coverage Start", header_cell_style), P("Coverage End", header_cell_style)]]
        for ins in insurance_hist:
            ins_data.append([
                P(ins.get("insurer", ""), cell_style),
                P(ins.get("start_date", ""), cell_style),
                P(ins.get("end_date", ""), cell_style)
            ])
        
        t = Table(ins_data, colWidths=[3*inch, 1.5*inch, 1.5*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 20: Lab Results/Observations =====
    observations = patient_info.get("observations", [])
    if observations:
        story.append(Paragraph(f"Lab Results & Other Observations ({len(observations)} total)", section_style))
        obs_data = [[P("Test/Observation", header_cell_style), P("Category", header_cell_style), P("Value", header_cell_style), P("Date", header_cell_style)]]
        for o in observations:
            obs_data.append([
                P(o.get("name", ""), cell_style),
                P(o.get("category", ""), cell_style),
                P(o.get("value", ""), cell_style),
                P(o.get("date", ""), cell_style)
            ])
        
        t = Table(obs_data, colWidths=[2.4*inch, 1*inch, 1.8*inch, 0.8*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION 21: Clinical Timeline (Key Events with Cross-References) =====
    timeline = patient_info.get("clinical_timeline", [])
    if timeline:
        story.append(Paragraph(f"Clinical Timeline - Key Events ({len(timeline)} events)", section_style))
        story.append(Paragraph("<i>Significant clinical events with related diagnoses and procedures</i>", styles['Normal']))
        story.append(Spacer(1, 6))
        
        for event in timeline:
            event_type = event.get("event_type", "")
            date = event.get("date", "")
            description = event.get("description", "")
            details = event.get("details", {})
            
            # Event header
            event_color = "darkred" if "Emergency" in event_type else ("darkblue" if "Hospital" in event_type else "darkgreen")
            story.append(Paragraph(f"<font color='{event_color}'><b>{date} - {event_type}</b></font>", styles['Normal']))
            story.append(Paragraph(f"  {description}", styles['Normal']))
            
            # Related conditions
            conditions = details.get("conditions_diagnosed", [])
            if conditions:
                cond_str = ", ".join(conditions[:3])
                story.append(Paragraph(f"  <i>Conditions diagnosed:</i> {cond_str}", styles['Normal']))
            
            # Related procedures
            procedures = details.get("procedures_performed", [])
            if procedures:
                proc_str = ", ".join(procedures[:3])
                story.append(Paragraph(f"  <i>Procedures performed:</i> {proc_str}", styles['Normal']))
            
            # Care plan activities
            activities = details.get("activities", [])
            if activities:
                act_str = ", ".join(activities[:3])
                story.append(Paragraph(f"  <i>Activities:</i> {act_str}", styles['Normal']))
            
            story.append(Spacer(1, 6))
    
    # ===== SECTION: Insurance/Coverage (moved to end) =====
    insurance = patient_info.get("insurance", [])
    if insurance:
        story.append(Paragraph(f"Insurance Coverage", section_style))
        ins_data = [[P("Insurer", header_cell_style), P("Coverage", header_cell_style)]]
        for ins in insurance:
            ins_data.append([P(ins.get("insurer", ""), cell_style_9), P(ins.get("coverage", ""), cell_style_9)])
        
        t = Table(ins_data, colWidths=[3*inch, 3*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightsteelblue),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION: Care Team (moved to end) =====
    care_team = patient_info.get("care_team", [])
    if care_team:
        story.append(Paragraph(f"Care Team & Healthcare Providers ({len(care_team)} total)", section_style))
        ct_data = [[P("Name", header_cell_style), P("Role", header_cell_style), P("Location", header_cell_style)]]
        for ct in care_team:
            ct_data.append([
                P(ct.get("name", ""), cell_style_9),
                P(ct.get("role", ""), cell_style_9),
                P(ct.get("location", ""), cell_style_9)
            ])
        
        t = Table(ct_data, colWidths=[2.5*inch, 2*inch, 1.5*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightcyan),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION: Healthcare Costs Summary (moved to end) =====
    costs = patient_info.get("healthcare_costs", {})
    if costs.get("total", 0) > 0:
        story.append(Paragraph(f"Healthcare Costs Summary", section_style))
        cost_data = [[P("Total Lifetime Healthcare Costs:", header_cell_style), P(f"${costs.get('total', 0):,.2f}", cell_style_9)]]
        t = Table(cost_data, colWidths=[3*inch, 3*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.lightsalmon),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        story.append(t)
        
        # Top costs by service type
        by_type = costs.get("by_type", [])
        if by_type:
            story.append(Spacer(1, 6))
            story.append(Paragraph("Top Costs by Service Type:", styles['Normal']))
            type_data = [[P("Service", header_cell_style), P("Cost", header_cell_style)]]
            for service, amount in by_type[:5]:
                type_data.append([P(service, cell_style), P(f"${amount:,.2f}", cell_style)])
            t = Table(type_data, colWidths=[4*inch, 2*inch])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightsalmon),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            story.append(t)
        story.append(Spacer(1, 12))
    
    # ===== SECTION: Care Plans (moved to end) =====
    care_plans = patient_info.get("care_plans", [])
    if care_plans:
        active_plans = [cp for cp in care_plans if cp.get("status") == "active"]
        story.append(Paragraph(f"Care Plans ({len(active_plans)} active, {len(care_plans)} total)", section_style))
        
        for cp in care_plans:
            status_color = "green" if cp.get("status") == "active" else "gray"
            plan_header = f"<b>{cp.get('name', 'Care Plan')}</b> - <font color='{status_color}'>{cp.get('status', '').upper()}</font>"
            story.append(Paragraph(plan_header, styles['Normal']))
            story.append(Paragraph(f"<i>Period: {cp.get('period', 'N/A')}</i>", styles['Normal']))
            
            # Activities
            activities = cp.get("activities", [])
            if activities:
                act_data = [[P("Activity", header_cell_style), P("Status", header_cell_style)]]
                for act in activities:
                    act_data.append([P(act.get("name", ""), cell_style), P(act.get("status", ""), cell_style)])
                t = Table(act_data, colWidths=[4.5*inch, 1.5*inch])
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ]))
                story.append(t)
            story.append(Spacer(1, 8))
    
    # ===== APPENDIX: Clinical Notes / Visit Summaries =====
    clinical_notes = patient_info.get("clinical_notes", [])
    if clinical_notes:
        story.append(PageBreak())
        story.append(Paragraph("Appendix: Clinical Notes & Visit Summaries", title_style))
        story.append(Paragraph(f"<i>Most recent {len(clinical_notes)} clinical notes</i>", notice_style))
        story.append(Spacer(1, 12))
        
        note_style = ParagraphStyle('NoteContent', parent=styles['Normal'], fontSize=8, leading=10, leftIndent=10)
        
        for i, note in enumerate(clinical_notes, 1):
            note_type = note.get("type", "Clinical Note")
            note_date = note.get("date", "")
            note_author = note.get("author", "")
            note_content = note.get("content", "")
            
            # Header for each note
            story.append(Paragraph(f"<b>Note {i}: {note_type}</b>", section_style))
            story.append(Paragraph(f"<i>Date: {note_date} | Author: {note_author}</i>", styles['Normal']))
            story.append(Spacer(1, 6))
            
            # Format the note content - preserve line breaks
            content_lines = note_content.split('\n')
            for line in content_lines:
                if line.strip():
                    # Escape special characters for ReportLab
                    safe_line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    story.append(Paragraph(safe_line, note_style))
            
            story.append(Spacer(1, 12))
    
    # Build PDF
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()
