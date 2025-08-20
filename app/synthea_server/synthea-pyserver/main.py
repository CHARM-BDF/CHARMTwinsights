from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import os
import subprocess
import tempfile
import shutil
import json
import glob
import requests
from pydantic import BaseModel
import logging

# Create a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# redirect / to the api docs
@app.get("/")
def redirect_to_docs():
    """Redirects the root URL to the API documentation."""
    return JSONResponse(status_code=307, content={"message": "Redirecting to /docs for API documentation."}, headers={"Location": "/docs"})


def run_synthea(num_patients, num_years, min_age=0, max_age=140, gender="both", exporter="fhir"):
    # Debug the incoming parameters
    print(f"\n==== RUN_SYNTHEA PARAMETERS ====")
    print(f"num_patients: {num_patients}, type: {type(num_patients)}")
    print(f"num_years: {num_years}, type: {type(num_years)}")
    print(f"min_age: {min_age}, type: {type(min_age)}")
    print(f"max_age: {max_age}, type: {type(max_age)}")
    print(f"gender: {gender}, type: {type(gender)}")
    print(f"exporter: {exporter}, type: {type(exporter)}")
    print(f"==== END RUN_SYNTHEA PARAMETERS ====\n")
    """ Runs Synthea to generate synthetic patient data.
    Args:
        num_patients: Number of synthetic patients to generate.
        num_years: Number of years of history to generate for each patient.
        min_age: Minimum age of generated patients (default: 0).
        max_age: Maximum age of generated patients (default: 140).
        gender: Gender of generated patients ("both", "male", or "female", default: "both").
        exporter: Export format, either 'csv' or 'fhir' (default: 'fhir').
    Returns:
        A tuple (temp_dir, output_dir) where:
        - temp_dir: Temporary directory where Synthea output is stored.
        - output_dir: Directory containing the generated resources (fhir or csv).
    Raises:
        Exception: If the output directory is not found."""
    
    temp_dir = tempfile.mkdtemp()
    # Calculate memory allocation based on patient count
    # Minimum 1GB, add 256MB per 100 patients, cap at 4GB
    memory_mb = min(4096, 1024 + (num_patients // 100) * 256)
    
    cmd = [
        "java", 
        f"-Xmx{memory_mb}m",  # Maximum heap size
        f"-Xms{memory_mb//2}m",  # Initial heap size (half of max)
        "-jar", "synthea-with-dependencies.jar",
        "-d", "modules",
        "--exporter.baseDirectory", temp_dir,
        "-p", str(num_patients),
        "--exporter.years_of_history", str(num_years)
    ]
    
    # Handle exporter format
    if exporter == "csv":
        cmd.append("--exporter.csv.export")
        cmd.append("true")
    elif exporter == "fhir":
        cmd.append("--exporter.fhir.export")
        cmd.append("true")
    else:
        # Default to FHIR if invalid exporter is provided
        cmd.append("--exporter.fhir.export")
        cmd.append("true")
        exporter = "fhir"
    
    # Handle age parameters
    if min_age != 0 or max_age != 140:
        cmd.extend(["-a", f"{min_age}-{max_age}"])
    
    # Handle gender parameter
    gender_norm = gender.strip().lower()
    gender_arg = None
    if gender_norm in ["m", "male"]:
        gender_arg = "M"
        print(f"Setting gender to Male (M) from input: {gender}")
    elif gender_norm in ["f", "female"]:
        gender_arg = "F"
        print(f"Setting gender to Female (F) from input: {gender}")
    else:
        print(f"Using default gender distribution (both) from input: {gender}")
    
    # Only add -g if gender is not 'both'
    if gender_arg:
        cmd.extend(["-g", gender_arg])
        print(f"Added gender flag: -g {gender_arg}")
    
    print(f"Full Synthea command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    # Determine the output directory based on the exporter
    output_dir = os.path.join(temp_dir, exporter)
    if not os.path.isdir(output_dir):
        raise Exception(f"{exporter.upper()} output directory not found!")
    return temp_dir, output_dir

# tags are organized as system: code, like {"urn:charm:cohort": "cohortA", "urn:charm:datatype": "synthetic"}

# input could be a resource or a bundle
# both resources and bundles should have a "meta" field and tags applied
# bundles can contain resources or bundles, so we need to work recursively
# tags are of the form system: code, e.g. {"urn:charm:cohort": "cohortA", "urn:charm:datatype": "synthetic"}
def apply_tags(resource, tags: dict[str, str] = None):
    """
    Recursively applies FHIR tags (in meta.tag) to all resources in a bundle or a single resource.
    Args:
        resource: dict representing a FHIR resource (could be Bundle or any resource)
        tags: dict of {system: code} to apply as tags
    """
    if tags is None:
        tags = {}

    # --- Step 1: Add tags to this resource's meta ---
    meta = resource.setdefault("meta", {})
    meta_tags = meta.setdefault("tag", [])

    # Index existing tags by system for easy update
    tag_index = {t["system"]: t for t in meta_tags if "system" in t and "code" in t}

    # Apply or update each tag
    for system, code in tags.items():
        if system in tag_index:
            tag_index[system]["code"] = code
        else:
            meta_tags.append({
                "system": system,
                "code": code
            })

    # --- Step 2: Recurse if this is a bundle ---
    if resource.get("resourceType") == "Bundle":
        for entry in resource.get("entry", []):
            entry_resource = entry.get("resource")
            if entry_resource:
                apply_tags(entry_resource, tags)

    # --- Step 3 (optional): Handle contained resources ---
    if "contained" in resource:
        for contained in resource["contained"]:
            apply_tags(contained, tags)



def fetch_group_by_id(hapi_url, group_id):
    """ Fetches a FHIR Group resource by ID from the HAPI FHIR server.
    Args:
        hapi_url: Base URL of the HAPI FHIR server.
        group_id: ID of the Group resource to fetch.
    Returns:
        The Group resource as a dictionary if found, None otherwise.
    """
    url = f"{hapi_url.rstrip('/')}/Group/{group_id}"
    print(f"Fetching group from URL: {url}")
    try:
        r = requests.get(url)
        print(f"Group fetch response status: {r.status_code}")
        if r.status_code == 200:
            group_data = r.json()
            print(f"Group data retrieved: ID={group_data.get('id')}, Type={group_data.get('resourceType')}")
            if 'member' in group_data:
                print(f"Group has {len(group_data['member'])} members")
            else:
                print("Group has no members")
            return group_data
        print(f"Failed to fetch group: Status {r.status_code}")
        return None
    except Exception as e:
        print(f"Error fetching group {group_id}: {e}")
        return None


def fetch_all_groups(hapi_url):
    """ Fetches all FHIR Group resources from the HAPI FHIR server.
    Args:
        hapi_url: Base URL of the HAPI FHIR server.
    Returns:
        A list of Group resources as dictionaries.
    """
    try:
        all_groups = []
        next_url = f"{hapi_url.rstrip('/')}/Group?_count=500"  # Increased count for efficiency
        
        # Keep fetching pages until there are no more
        while next_url:
            print(f"Fetching groups from: {next_url}")
            r = requests.get(next_url)
            if r.status_code != 200:
                print(f"Error fetching groups: HTTP {r.status_code}")
                break
                
            bundle = r.json()
            
            # Extract groups from this page
            if "entry" in bundle:
                page_groups = [entry["resource"] for entry in bundle["entry"]]
                all_groups.extend(page_groups)
                print(f"Retrieved {len(page_groups)} groups from this page. Total so far: {len(all_groups)}")
            
            # Look for the 'next' link to continue pagination
            next_url = None
            if "link" in bundle:
                for link in bundle["link"]:
                    if link.get("relation") == "next" and "url" in link:
                        next_url = link["url"]
                        break
        
        print(f"Total groups retrieved: {len(all_groups)}")
        return all_groups
    except Exception as e:
        print(f"Error fetching groups: {e}")
        return []


def fetch_all_patients(hapi_url):
    """ Fetches all FHIR Patient resources from the HAPI FHIR server.
    Args:
        hapi_url: Base URL of the HAPI FHIR server.
    Returns:
        A list of Patient resources as dictionaries.
    """
    try:
        all_patients = []
        next_url = f"{hapi_url.rstrip('/')}/Patient?_count=500"  # Increased count for efficiency
        
        # Keep fetching pages until there are no more
        while next_url:
            print(f"Fetching patients from: {next_url}")
            r = requests.get(next_url)
            if r.status_code != 200:
                print(f"Error fetching patients: HTTP {r.status_code}")
                break
                
            bundle = r.json()
            
            # Extract patients from this page
            if "entry" in bundle:
                page_patients = [entry["resource"] for entry in bundle["entry"]]
                all_patients.extend(page_patients)
                print(f"Retrieved {len(page_patients)} patients from this page. Total so far: {len(all_patients)}")
            
            # Look for the 'next' link to continue pagination
            next_url = None
            if "link" in bundle:
                for link in bundle["link"]:
                    if link.get("relation") == "next" and "url" in link:
                        next_url = link["url"]
                        break
        
        print(f"Total patients retrieved: {len(all_patients)}")
        return all_patients
    except Exception as e:
        print(f"Error fetching patients: {e}")
        return []


def merge_group_members(existing_group, new_patient_ids):
    """
    Merges new patient IDs into an existing Group resource's member list.
    """
    # Existing member patient IDs
    existing_member_ids = set()
    for member in existing_group.get("member", []):
        ref = member.get("entity", {}).get("reference", "")
        if ref.startswith("Patient/"):
            existing_member_ids.add(ref.split("/", 1)[1])
    # Merge with new patients
    all_ids = existing_member_ids | set(new_patient_ids)
    # Replace the member array with merged list
    existing_group["member"] = [{"entity": {"reference": f"Patient/{pid}"}} for pid in all_ids]
    return existing_group



def post_bundle(json_file, hapi_url, tags: dict[str, str] = None): # returns (success (bool), message (str), patient_ids (set of str) or None)
    """ Posts a FHIR Bundle or resource to the HAPI FHIR server. Returned patient_ids is a set of patient IDs found in the bundle, useful for cohort management.
    Args:
        json_file: Path to the JSON file containing the FHIR Bundle or resource.
        hapi_url: Base URL of the HAPI FHIR server (e.g., http://hapi:8080/fhir).
        tags: Optional dictionary of tags to apply to the resource or bundle.
    Returns:
        A tuple (success, message, patient_ids) where:
        - success (bool): True if the post was successful, False otherwise.
        - message (dict): Response text or detailed error information.
        - patient_ids (set of str): Set of patient IDs found in the bundle, or None if no patients were found.
    """
    patient_ids = set()
    with open(json_file, "r") as f:
        bundle = json.load(f)

        # collect patient IDs
        if bundle.get("resourceType") == "Bundle" and "entry" in bundle:
            for entry in bundle["entry"]:
                if "resource" in entry and entry["resource"].get("resourceType") == "Patient":
                    patient_id = entry["resource"].get("id")
                    if patient_id:
                        patient_ids.add(patient_id)
         
        if tags:
            apply_tags(bundle, tags)

    bundle_type = bundle.get("type")
    # Decide endpoint based on bundle type
    if bundle_type in ("transaction", "batch"):
        url = hapi_url  # base URL, e.g. http://hapi:8080/fhir
    else:
        url = hapi_url.rstrip("/") + "/Bundle"
    try:
        # Add timeout for large bundles - calculate based on bundle size
        bundle_size = len(json.dumps(bundle))
        # 2 seconds per 10KB with a minimum of 15 seconds and maximum of 180 seconds
        timeout = max(15, min(180, bundle_size / 5000))
        logger.info(f"Posting bundle {os.path.basename(json_file)} (size: {bundle_size/1024:.1f}KB) with timeout {timeout:.1f}s")
        
        # Use session for connection pooling and performance
        session = requests.Session()
        
        # Add retry mechanism with exponential backoff
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,  # Maximum number of retries
            backoff_factor=1,  # Exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
            allowed_methods=["POST"]  # Only retry POST requests
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        r = session.post(
            url, 
            json=bundle, 
            headers={"Content-Type": "application/fhir+json"}, 
            timeout=timeout
        )
        r.raise_for_status()
        return True, {"response": r.text}, patient_ids
    except requests.Timeout:
        error_info = {
            "error_type": "timeout",
            "file_name": os.path.basename(json_file),
            "bundle_size_kb": round(bundle_size/1024, 1),
            "timeout_seconds": round(timeout, 1),
            "message": f"Timeout posting bundle to HAPI server after {timeout} seconds"
        }
        print(f"Timeout error: {error_info['message']}")
        return False, error_info, None
    except requests.HTTPError as e:
        error_body = r.text if 'r' in locals() else "No response body"
        status_code = r.status_code if 'r' in locals() else "unknown"
        error_info = {
            "error_type": "http_error",
            "file_name": os.path.basename(json_file),
            "status_code": status_code,
            "message": f"HTTP error {status_code} posting bundle",
            "response_body": error_body[:500] if len(error_body) > 500 else error_body
        }
        print(f"HTTP error: {error_info['message']}")
        return False, error_info, None
    except Exception as e:
        error_info = {
            "error_type": "general_error",
            "file_name": os.path.basename(json_file),
            "exception": str(e.__class__.__name__),
            "message": str(e)
        }
        print(f"General error: {error_info['message']}")
        return False, error_info, None
    

def upsert_group(hapi_url, cohort_id, new_patient_ids, tags):
    """ Upserts a FHIR Group resource with the given cohort ID and patient IDs.
    If the Group already exists, it merges the new patient IDs with existing members.
    If creating a new Group, adds a creation timestamp tag.
    Args:
        hapi_url: Base URL of the HAPI FHIR server (e.g., http://hapi:8080/fhir).
        cohort_id: The ID of the cohort to create or update.
        new_patient_ids: A set of new patient IDs to add to the Group.
        tags: Optional dictionary of tags to apply to the Group resource.
    Returns:
        The response text from the HAPI FHIR server after the upsert operation.
    Raises:
        RuntimeError: If there is an error fetching or updating the Group resource."""
    # Try to fetch existing Group
    url = f"{hapi_url.rstrip('/')}/Group/{cohort_id}"
    existing_ids = set()
    group_exists = False
    try:
        r = requests.get(url, headers={"Accept": "application/fhir+json"})
        if r.status_code == 200:
            group = r.json()
            group_exists = True
            for member in group.get("member", []):
                ref = member.get("entity", {}).get("reference", "")
                if ref.startswith("Patient/"):
                    existing_ids.add(ref.split("/", 1)[1])
        elif r.status_code != 404:
            r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Error fetching Group/{cohort_id}: {e}")

    # Merge new and existing patient ids
    all_ids = existing_ids | set(new_patient_ids)

    # Get current time in ISO format for the creation timestamp
    import datetime
    current_time = datetime.datetime.now().isoformat()

    group = {
        "resourceType": "Group",
        "id": cohort_id,
        "type": "person",
        "actual": True,
        "member": [{"entity": {"reference": f"Patient/{pid}"}} for pid in all_ids],
        "meta": {
            "tag": [
                {"system": "urn:charm:cohort", "code": cohort_id},
                {"system": "urn:charm:datatype", "code": "synthetic"},
                {"system": "urn:charm:source", "code": "synthea"}
            ]
        }
    }
    
    # Add creation timestamp tag if this is a new group
    if not group_exists:
        group["meta"]["tag"].append({
            "system": "urn:charm:created",
            "code": current_time
        })
        print(f"Adding creation timestamp {current_time} to new cohort {cohort_id}")
    if tags:
        apply_tags(group, tags)
    r = requests.put(url, json=group, headers={"Content-Type": "application/fhir+json"})
    r.raise_for_status()
    return r.text


class SyntheaRequest(BaseModel):
    num_patients: int = 10
    num_years: int = 1
    cohort_id: str = "default"
    exporter: str = "fhir"
    min_age: int = 0
    max_age: int = 140
    gender: str = "both"

@app.post("/synthetic-patients")
async def push_patients(request: SyntheaRequest):
    # Debug logging for request parameters
    print(f"\n==== REQUEST DEBUG INFO ====")
    print(f"Received request with parameters:")
    print(f"  - num_patients: {request.num_patients}")
    print(f"  - num_years: {request.num_years}")
    print(f"  - cohort_id: {request.cohort_id}")
    print(f"  - exporter: {request.exporter}")
    print(f"  - min_age: {request.min_age}")
    print(f"  - max_age: {request.max_age}")
    print(f"  - gender: {request.gender}")
    print(f"==== END DEBUG INFO ====\n")
    
    # Also try to get the raw request body
    from fastapi import Request
    from fastapi.concurrency import run_in_threadpool
    import inspect
    
    # Get the raw request
    request_obj = inspect.currentframe().f_locals.get('request')
    if isinstance(request_obj, Request):
        try:
            body = await request_obj.body()
            print(f"Raw request body: {body.decode()}")
        except Exception as e:
            print(f"Error getting raw body: {e}")
    else:
        print("Could not access raw request")
    """ Pushes synthetic patient data generated by Synthea to a HAPI FHIR server.
    Args:
        num_patients: Number of synthetic patients to generate (default: 10).
        num_years: Number of years of history to generate for each patient (default: 1).
        cohort_id: Optional ID for the cohort to which these patients belong. If provided, a FHIR Group resource will be created or updated with these patients.
        exporter: Export format, either 'csv' or 'fhir' (default: 'fhir').
        min_age: Minimum age of generated patients (default: 0).
        max_age: Maximum age of generated patients (default: 140).
        gender: Gender of generated patients ('both', 'male', or 'female', default: 'both').
    Returns:
        A summary of the operation including successful and failed bundles, patient IDs, and tags applied.
    """
    if request.num_patients <= 0:
        return JSONResponse(status_code=400, content={"error": "num_patients must be a positive integer."})
    if request.num_years <= 0:
        return JSONResponse(status_code=400, content={"error": "num_years must be a positive integer."})

    hapi_url = "http://hapi:8080/fhir"
    # we need to check if the hapi server is running by checking fhir/$meta, return an error if result is not 200; no retries
    try:
        r = requests.get(hapi_url + "/$meta")
        r.raise_for_status()
    except Exception as e:
        # we'll return a 500 error with the messag se
        ret = JSONResponse(status_code=500, content={"error": f"HAPI FHIR server is not reachable. (It may be starting up.)"})
        return ret

    # Check if the exporter is valid
    if request.exporter not in ["csv", "fhir"]:
        return JSONResponse(status_code=400, content={"error": "Invalid exporter. Must be 'csv' or 'fhir'."})    
    
    print(f"\n==== BEFORE CALLING RUN_SYNTHEA ====")
    print(f"About to call run_synthea with:")
    print(f"num_patients={request.num_patients}, num_years={request.num_years}")
    print(f"min_age={request.min_age}, max_age={request.max_age}")
    print(f"gender={request.gender}, exporter={request.exporter}")
    print(f"==== END BEFORE CALLING ====\n")
    
    temp_dir, output_dir = run_synthea(
        request.num_patients, 
        request.num_years,
        request.min_age,
        request.max_age,
        request.gender,
        request.exporter
    )
    
    print("\n==== AFTER CALLING RUN_SYNTHEA ====")
    print(f"Returned temp_dir: {temp_dir}")
    print(f"Returned output_dir: {output_dir}")
    print("==== END AFTER CALLING ====\n")

    tagset = {"urn:charm:cohort": request.cohort_id, "urn:charm:datatype": "synthetic", "urn:charm:source": "synthea"} 

    # Get current time in ISO format for the creation timestamp
    import datetime
    current_time = datetime.datetime.now().isoformat()
    
    # Add creation timestamp to tagset
    tagset["urn:charm:created"] = current_time
    
    try:
        # 1. Practitioner and hospital info first
        special_files = sorted(glob.glob(os.path.join(output_dir, "practitionerInformation*.json"))) + \
                        sorted(glob.glob(os.path.join(output_dir, "hospitalInformation*.json")))
        all_files = sorted(glob.glob(os.path.join(output_dir, "*.json")))
        patient_files = [f for f in all_files if f not in special_files]

        results = []
        # First special files
        for json_file in special_files:
            success, error_info, _ = post_bundle(json_file, hapi_url, tags=tagset)
            results.append({"file": os.path.basename(json_file), "success": success, "msg": error_info})

        # Then patient bundles - process in batches for better error handling
        patient_ids = set()
        total_files = len(patient_files)
        print(f"Processing {total_files} patient files")
        
        # Process files in batches to avoid overwhelming the server
        # Adjust batch size based on number of patients - smaller batches for larger cohorts
        batch_size = max(5, min(20, 100 // (total_files // 10 + 1)))  # Dynamic batch sizing
        
        # Add retry logic for failed uploads
        max_retries = 3
        retry_delay = 5  # seconds
        
        for i in range(0, total_files, batch_size):
            batch = patient_files[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size} ({len(batch)} files)")
            
            for json_file in batch:
                # Try up to max_retries times
                for retry in range(max_retries):
                    try:
                        success, error_info, new_patient_ids = post_bundle(json_file, hapi_url, tags=tagset)
                        if new_patient_ids:
                            patient_ids.update(new_patient_ids)
                        results.append({"file": os.path.basename(json_file), "success": success, "msg": error_info})
                        break  # Success, exit retry loop
                    except Exception as e:
                        if retry < max_retries - 1:
                            logger.warning(f"Retry {retry+1}/{max_retries} for {os.path.basename(json_file)}: {str(e)}")
                            import time
                            time.sleep(retry_delay)
                        else:
                            # Last retry failed
                            logger.error(f"Failed to upload {os.path.basename(json_file)} after {max_retries} attempts: {str(e)}")
                            results.append({"file": os.path.basename(json_file), "success": False, "msg": f"Failed after {max_retries} attempts: {str(e)}"})
            
            # Adaptive delay between batches based on batch size
            if i + batch_size < total_files:
                import time
                delay = min(5, max(1, batch_size / 5))  # 1-5 seconds based on batch size
                print(f"Pausing for {delay:.1f} seconds between batches...")
                time.sleep(delay)


        try:
            msg = upsert_group(hapi_url, request.cohort_id, patient_ids, tagset)
            results.append({"file": f"group_{request.cohort_id}", "success": True, "msg": msg})
        except Exception as e:
            results.append({"file": f"group_{request.cohort_id}", "success": False, "msg": str(e)})

        # Collect detailed information about failed bundles
        failed_bundles = [r for r in results if not r["success"]]
        failure_details = []
        
        for failed in failed_bundles:
            # Extract the error details
            error_info = failed.get("msg", {})
            if isinstance(error_info, dict):
                failure_details.append(error_info)
            else:
                # Handle legacy string error messages
                failure_details.append({
                    "file_name": failed.get("file", "unknown"),
                    "error_type": "unknown",
                    "message": str(error_info)
                })
        
        summary = {
            "successful_bundles": sum(1 for r in results if r["success"]),
            "failed_bundles": sum(1 for r in results if not r["success"]), 
            "patient_ids": list(patient_ids),
            "num_patients": len(patient_ids),
            "cohort_id": request.cohort_id,  # Explicitly include the cohort ID in the response
            "tags_applied": tagset,
            "failure_details": failure_details if failure_details else None
        }
        return summary
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@app.get("/list-all-patients", response_class=JSONResponse)
async def list_all_patients():
    """ Lists all patients stored in the HAPI FHIR server with specific demographic information.
    Returns:
        A JSON object containing a list of patients with their IDs, gender, ethnicity, date of birth, and cohort IDs
    """
    # Get the HAPI URL from environment variable
    hapi_url = os.environ.get('HAPI_URL')
    if not hapi_url:
        hapi_url = "http://hapi:8080/fhir"
        print(f"HAPI_URL not set, using default: {hapi_url}")
    
    # Check if the HAPI server is accessible
    try:
        r = requests.get(f"{hapi_url}/$meta", timeout=5)
        r.raise_for_status()
    except Exception as e:
        error_msg = f"HAPI FHIR server is not reachable: {str(e)}"
        print(error_msg)
        return JSONResponse(
            status_code=500, 
            content={"error": error_msg}
        )
    
    try:
        # Fetch all groups/cohorts
        print("Fetching groups from HAPI server...")
        groups = fetch_all_groups(hapi_url)
        print(f"Found {len(groups)} groups/cohorts")
        
        # Create a mapping of patient IDs to cohorts
        patient_to_cohorts = {}
        cohort_info = []
        
        # Process each group/cohort
        for group in groups:
            try:
                cohort_id = group.get("id")
                cohort_name = group.get("name", cohort_id)
                
                # Get tags if available
                tags = {}
                if "meta" in group and "tag" in group["meta"]:
                    for tag in group["meta"]["tag"]:
                        if "system" in tag and "code" in tag:
                            tags[tag["system"]] = tag["code"]
                
                # Get members
                members = []
                if "member" in group:
                    for member in group["member"]:
                        if "entity" in member and "reference" in member["entity"]:
                            patient_ref = member["entity"]["reference"]
                            if patient_ref.startswith("Patient/"):
                                patient_id = patient_ref[8:]  # Remove "Patient/" prefix
                                members.append(patient_id)
                                
                                # Add this cohort to the patient's list of cohorts
                                if patient_id not in patient_to_cohorts:
                                    patient_to_cohorts[patient_id] = []
                                patient_to_cohorts[patient_id].append({
                                    "cohort_id": cohort_id,
                                    "cohort_name": cohort_name
                                })
                
                # Add cohort info to the list
                cohort_info.append({
                    "cohort_id": cohort_id,
                    "name": cohort_name,
                    "member_count": len(members),
                    "tags": tags
                })
            except Exception as e:
                print(f"Error processing group {group.get('id', 'unknown')}: {str(e)}")
        
        # Fetch all patients to ensure we include those not in any cohort
        print("Fetching patients from HAPI server...")
        patients = fetch_all_patients(hapi_url)
        print(f"Found {len(patients)} patients")
        
        # Create the final patient list
        patient_list = []
        for patient in patients:
            try:
                patient_id = patient.get("id")
                if not patient_id:
                    continue
                
                # Get birth date if available
                birth_date = patient.get("birthDate", "unknown")
                
                # Get cohorts from Group memberships
                cohorts = patient_to_cohorts.get(patient_id, [])
                cohort_ids = [c.get("cohort_id") for c in cohorts]
                
                # ALSO check for cohort tags in the patient's metadata
                if "meta" in patient and "tag" in patient["meta"]:
                    for tag in patient["meta"]["tag"]:
                        if tag.get("system") == "urn:charm:cohort":
                            cohort_id = tag.get("code")
                            if cohort_id not in cohort_ids:
                                cohort_ids.append(cohort_id)
                
                # Get gender if available
                gender = patient.get("gender", "unknown")
                
                # Extract ethnicity from extensions
                ethnicity = "unknown"
                if "extension" in patient:
                    for ext in patient["extension"]:
                        # Look for US Core ethnicity extension
                        if ext.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                            # Extract text representation if available
                            for nested_ext in ext.get("extension", []):
                                if nested_ext.get("url") == "text" and "valueString" in nested_ext:
                                    ethnicity = nested_ext["valueString"]
                                    break
                        # Alternative: look for direct ethnicity extension
                        elif ext.get("url") == "http://hl7.org/fhir/StructureDefinition/patient-ethnicity":
                            if "valueCodeableConcept" in ext and "text" in ext["valueCodeableConcept"]:
                                ethnicity = ext["valueCodeableConcept"]["text"]
                            elif "valueString" in ext:
                                ethnicity = ext["valueString"]
                
                # Add to patient list with only the requested fields
                patient_info = {
                    "id": patient_id,
                    "gender": gender,
                    "ethnicity": ethnicity,
                    "birth_date": birth_date,
                    "cohort_ids": cohort_ids
                }
                
                patient_list.append(patient_info)
            except Exception as e:
                print(f"Error processing patient {patient.get('id', 'unknown')}: {str(e)}")
        
        return {
            "patients": patient_list,
            "total_patients": len(patient_list)
        }
    except Exception as e:
        error_msg = f"Error processing patients and cohorts: {str(e)}"
        print(error_msg)
        return JSONResponse(
            status_code=500, 
            content={"error": error_msg}
        )


@app.get("/modules", response_class=JSONResponse)
async def get_synthea_modules_list():
    try:
        # Access the shared volume path directly
        modules_path = "modules"
        
        if not os.path.exists(modules_path):
            return {
                "modules": {},
                "count": 0,
                "error": f"Path {modules_path} not found"
            }
        
        # Function to collect JSON files recursively
        def find_json_files(directory):
            json_files = []
            for root, dirs, files in os.walk(directory):
                for file in files:
                    if file.endswith('.json'):
                        # Get relative path from base modules directory
                        rel_path = os.path.relpath(os.path.join(root, file), modules_path)
                        json_files.append((rel_path, os.path.join(root, file)))
            return json_files
        
        # Get all JSON files recursively
        module_files = find_json_files(modules_path)
        
        # Create a dictionary to store module information
        modules_info = {}
        
        for rel_path, file_path in module_files:
            # Extract information from the JSON file
            try:
                with open(file_path, 'r') as f:
                    import json
                    module_json = json.load(f)
                    
                    module_info = {
                        "name": os.path.basename(file_path),
                        "path": rel_path
                    }
                    
                    # Look for remarks field (case insensitive)
                    remarks = None
                    for key in module_json:
                        if key.lower() == "remarks":
                            remarks = module_json[key]
                            break
                    
                    # If remarks exist, join them if it's a list, otherwise convert to string
                    if isinstance(remarks, list):
                        remarks_text = "\n".join(remarks)
                    elif remarks:
                        remarks_text = str(remarks)
                    else:
                        remarks_text = ""
                    
                    # Check if remarks indicate a blank module or is empty
                    if not remarks_text or "blank module" in remarks_text.lower() or "empty module" in remarks_text.lower():
                        module_info["description"] = "No description provided"
                    else:
                        module_info["description"] = remarks_text
                    
                    # Count states and transitions
                    states_count = 0
                    transitions_count = 0
                    
                    # Count states
                    states = module_json.get("states", {})
                    if isinstance(states, dict):
                        states_count = len(states)
                        
                        # Count transitions by examining each state
                        for state_name, state_data in states.items():
                            # Direct transition
                            if "direct_transition" in state_data:
                                transitions_count += 1
                            
                            # Distributed transition
                            elif "distributed_transition" in state_data:
                                if isinstance(state_data["distributed_transition"], list):
                                    transitions_count += len(state_data["distributed_transition"])
                            
                            # Conditional transition
                            elif "conditional_transition" in state_data:
                                if isinstance(state_data["conditional_transition"], list):
                                    transitions_count += len(state_data["conditional_transition"])
                            
                            # Complex transition
                            elif "complex_transition" in state_data:
                                if isinstance(state_data["complex_transition"], list):
                                    transitions_count += len(state_data["complex_transition"])
                            
                            # Table transition
                            elif "table_transition" in state_data:
                                transitions_count += 1  # Count as one transition since we can't easily count rows
                    
                    module_info["states_count"] = states_count
                    module_info["transitions_count"] = transitions_count
                    
                    # Add module to dictionary with relative path as key
                    # Use rel_path directly as the key
                    modules_info[rel_path] = module_info
                    
            except Exception as e:
                # If we can't read the file, add basic info
                module_info = {
                    "name": os.path.basename(file_path),
                    "path": rel_path,
                    "description": "No description provided",
                    "states_count": 0,
                    "transitions_count": 0,
                    "error": str(e)
                }
                modules_info[rel_path] = module_info
        
        return {
            "modules": modules_info,
            "count": len(modules_info),
            "path": modules_path
        }
        
    except Exception as e:
        logging.error(f"Error accessing modules: {str(e)}", exc_info=True)
        return {
            "modules": {},
            "count": 0,
            "error": str(e)
        }
    
    

@app.get("/modules/{module_name}", response_class=JSONResponse)
async def get_module_content(module_name: str):
    try:
        # Ensure module_name has .json extension
        if not module_name.endswith('.json'):
            module_name += '.json'
            
        # Access the shared volume path directly
        modules_path = "modules"
        
        if not os.path.exists(modules_path):
            raise HTTPException(status_code=404, detail=f"Modules path {modules_path} not found")
        
        # Search for the module file recursively
        found_path = None
        
        for root, dirs, files in os.walk(modules_path):
            if module_name in files:
                found_path = os.path.join(root, module_name)
                break
            
        if not found_path:
            raise HTTPException(status_code=404, detail=f"Module '{module_name}' not found")
        
        # Read the module file
        try:
            with open(found_path, 'r') as f:
                import json
                module_content = json.load(f)
                
                # Get relative path from modules directory
                rel_path = os.path.relpath(found_path, modules_path)
                
                # Return module content along with metadata
                return {
                    "name": module_name,
                    "path": rel_path,
                    "full_path": found_path,
                    "content": module_content
                }
                
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Error parsing module file: {str(e)}"
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail=f"Error reading module file: {str(e)}"
            )
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
        
    except Exception as e:
        logging.error(f"Error accessing module: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )

@app.get("/list-all-cohorts", response_class=JSONResponse)
async def list_all_cohorts():
    """ Lists all cohorts stored in the HAPI FHIR server along with the number of patients in each cohort and their source.
    Returns:
        A JSON object containing a list of cohorts with their IDs, patient counts, and sources.
    """
    hapi_url = "http://hapi:8080/fhir"
    
    # Check if the HAPI server is running
    try:
        r = requests.get(hapi_url + "/$meta")
        r.raise_for_status()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"HAPI FHIR server is not reachable. (It may be starting up.)"})
    
    # Fetch all groups from the HAPI server
    all_groups = fetch_all_groups(hapi_url)
    
    # Process the groups to extract cohort information
    cohorts = []
    for group in all_groups:
        # Extract cohort ID, source, and creation time from tags
        cohort_id = None
        source = None
        creation_time = None
        
        # First check if this is a cohort by looking at the ID directly
        group_id = group.get("id")
        
        # Look for cohort information in tags
        if "meta" in group and "tag" in group["meta"]:
            for tag in group["meta"]["tag"]:
                if tag.get("system") == "urn:charm:cohort":
                    cohort_id = tag.get("code")
                if tag.get("system") == "urn:charm:source":
                    source = tag.get("code")
                if tag.get("system") == "urn:charm:created":
                    creation_time = tag.get("code")
                # Also check for datatype tag to identify synthetic cohorts
                if tag.get("system") == "urn:charm:datatype" and tag.get("code") == "synthetic":
                    # If we have a synthetic datatype but no cohort ID, use the group ID
                    if not cohort_id and group_id:
                        cohort_id = group_id
                        print(f"Using group ID {group_id} as cohort ID for synthetic cohort")
        
        # Skip if this is not a cohort group
        if not cohort_id:
            continue
        
        # Count the number of patients in the group
        patient_count = 0
        if "member" in group:
            patient_count = len(group["member"])
        
        # Add cohort info to the list
        cohort_info = {
            "cohort_id": cohort_id,
            "patient_count": patient_count,
            "source": source or "unknown"
        }
        
        # Include creation time if available
        if creation_time:
            cohort_info["created_at"] = creation_time
        else:
            cohort_info["created_at"] = "unknown"
            
        cohorts.append(cohort_info)
    
    return {
        "cohorts": cohorts,
        "total_cohorts": len(cohorts)
    }

def fetch_complete_patient_data(hapi_url, patient_id=None):
    """ Fetches complete patient data including all related resources.
    
    This function retrieves a patient's complete clinical record by:
    1. Getting the patient demographic data
    2. Getting all resources that reference the patient
    
    Args:
        hapi_url: Base URL of the HAPI FHIR server
        patient_id: Optional specific patient ID to fetch. If None, fetches all patients.
        
    Returns:
        A list of dictionaries, each containing a patient's complete data
    """
    try:
        # First get all patients or a specific patient
        if patient_id:
            url = f"{hapi_url}/Patient/{patient_id}"
            print(f"Fetching patient data from {url}")
            r = requests.get(url)
            
            # Check if patient exists
            if r.status_code == 404:
                print(f"Patient with ID {patient_id} not found")
                return []
                
            r.raise_for_status()
            patients = [r.json()]
            print(f"Successfully fetched patient {patient_id}")
        else:
            patients = fetch_all_patients(hapi_url)
            print(f"Fetched {len(patients)} patients")
        
        complete_patient_data = []
        
        # For each patient, get all resources that reference this patient
        for patient in patients:
            patient_id = patient.get("id")
            if not patient_id:
                print("Skipping patient with no ID")
                continue
                
            print(f"Processing patient {patient_id}")
            patient_data = {
                "demographics": patient,
                "resources": {}
            }
            
            # List of resource types to fetch for each patient
            resource_types = [
                "Condition", "Observation", "Procedure", 
                "MedicationRequest", "MedicationAdministration",
                "Encounter", "AllergyIntolerance", "Immunization",
                "DiagnosticReport", "CarePlan", "Claim"
            ]
            
            # Fetch each resource type for this patient
            for resource_type in resource_types:
                try:
                    url = f"{hapi_url}/{resource_type}?patient=Patient/{patient_id}"
                    r = requests.get(url)
                    r.raise_for_status()
                    bundle = r.json()
                    
                    if "entry" in bundle:
                        resources = [entry["resource"] for entry in bundle["entry"]]
                        patient_data["resources"][resource_type] = resources
                        print(f"Found {len(resources)} {resource_type} resources for patient {patient_id}")
                    else:
                        patient_data["resources"][resource_type] = []
                        print(f"No {resource_type} resources found for patient {patient_id}")
                        
                except Exception as e:
                    print(f"Error fetching {resource_type} for patient {patient_id}: {e}")
                    patient_data["resources"][resource_type] = []
            
            complete_patient_data.append(patient_data)
            
        print(f"Completed processing {len(complete_patient_data)} patients with their resources")
        return complete_patient_data
    except Exception as e:
        print(f"Error in fetch_complete_patient_data: {e}")
        return []


def extract_leaf_keys(data, prefix="", result=None, value_counts=None):
    """ Recursively extracts all leaf keys from a nested JSON structure and tracks their values.
    
    Args:
        data: The JSON data to extract keys from
        prefix: Current key prefix for nested structures
        result: Dictionary to collect key counts
        value_counts: Dictionary to collect value frequencies for each key
        
    Returns:
        Tuple of (key_counts, value_counts) where:
        - key_counts is a dictionary with leaf keys as keys and their counts as values
        - value_counts is a dictionary with leaf keys as keys and a Counter of their values as values
    """
    if result is None:
        result = {}
    if value_counts is None:
        value_counts = {}
        
    if isinstance(data, dict):
        for key, value in data.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (dict, list)):
                extract_leaf_keys(value, new_prefix, result, value_counts)
            else:
                # Count the key
                if new_prefix in result:
                    result[new_prefix] += 1
                else:
                    result[new_prefix] = 1
                
                # Track the value
                str_value = str(value)[:100]  # Truncate very long values
                if new_prefix not in value_counts:
                    value_counts[new_prefix] = {}
                if str_value in value_counts[new_prefix]:
                    value_counts[new_prefix][str_value] += 1
                else:
                    value_counts[new_prefix][str_value] = 1
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, (dict, list)):
                extract_leaf_keys(item, prefix, result, value_counts)
                
    return result, value_counts


@app.get("/count-patient-keys", response_class=JSONResponse)
async def count_patient_keys(cohort_id: str = None):
    """ Counts the occurrence of leaf keys in patient JSON data including all related resources.
    
    Args:
        cohort_id: Optional ID of the cohort to analyze. If not provided, all patients are analyzed.
        
    Returns:
        A JSON object containing counts of leaf keys across all patients in the specified cohort or all patients,
        along with the 3 most common values for each key.
    """
    hapi_url = "http://hapi:8080/fhir"
    
    # Check if the HAPI server is running
    try:
        r = requests.get(hapi_url + "/$meta")
        r.raise_for_status()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"HAPI FHIR server is not reachable. (It may be starting up.)"})
    
    # If cohort_id is provided, get patient IDs with the cohort tag
    patient_ids = None
    if cohort_id:
        # Query for patients with the specific cohort tag
        cohort_tag = f"urn:charm:cohort|{cohort_id}"
        print(f"Searching for patients with tag: {cohort_tag}")
        
        try:
            # Use _tag parameter to find patients with this cohort tag
            url = f"{hapi_url}/Patient?_tag={cohort_tag}&_count=1000"
            print(f"Querying URL: {url}")
            r = requests.get(url)
            r.raise_for_status()
            bundle = r.json()
            
            # Extract patient IDs from the bundle
            patient_ids = []
            if "entry" in bundle:
                for entry in bundle["entry"]:
                    if "resource" in entry and entry["resource"].get("resourceType") == "Patient":
                        patient_id = entry["resource"].get("id")
                        if patient_id:
                            patient_ids.append(patient_id)
            
            print(f"Found {len(patient_ids)} patients with cohort tag '{cohort_id}'")
            print(f"Patient IDs in cohort: {patient_ids[:5]}{'...' if len(patient_ids) > 5 else ''}")
            
            # If cohort is empty, return early
            if not patient_ids:
                return {
                    "total_patients": 0,
                    "cohort_id": cohort_id,
                    "key_analysis": {}
                }
        except Exception as e:
            print(f"Error retrieving patients for cohort '{cohort_id}': {e}")
            return JSONResponse(status_code=500, content={"error": f"Error retrieving patients for cohort '{cohort_id}': {str(e)}"})
    
    # Get complete patient data for all patients or filtered by cohort
    print(f"Fetching complete patient data...")
    all_patient_data = []
    
    if cohort_id and patient_ids:
        print(f"Analyzing {len(patient_ids)} patients in cohort '{cohort_id}'")
        print(f"Patient IDs in cohort: {patient_ids[:5]}{'...' if len(patient_ids) > 5 else ''}")
        
        # Process patients in batches to avoid overloading the server
        batch_size = 5  # Process 5 patients at a time
        
        for i in range(0, len(patient_ids), batch_size):
            batch = patient_ids[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(patient_ids) + batch_size - 1)//batch_size}")
            
            for patient_id in batch:
                # Fetch complete data for this patient
                print(f"Fetching data for patient {patient_id}")
                patient_data = fetch_complete_patient_data(hapi_url, patient_id)
                print(f"Patient {patient_id} data retrieved: {bool(patient_data)} (length: {len(patient_data) if patient_data else 0})")
                if patient_data:  # Make sure we got data back
                    all_patient_data.extend(patient_data)
    else:
        # For all patients, we'll use a different approach to avoid memory issues
        # First get basic patient data
        all_patients = fetch_all_patients(hapi_url)
        print(f"Retrieved {len(all_patients)} patients for key analysis")
        
        # Process patients in batches
        batch_size = 5
        
        for i in range(0, len(all_patients), batch_size):
            batch = all_patients[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(len(all_patients) + batch_size - 1)//batch_size}")
            
            for patient in batch:
                patient_id = patient.get("id")
                if patient_id:
                    print(f"Fetching data for patient {patient_id}")
                    patient_data = fetch_complete_patient_data(hapi_url, patient_id)
                    if patient_data:  # Make sure we got data back
                        all_patient_data.extend(patient_data)
    
    print(f"Retrieved complete data for {len(all_patient_data)} patients")
    
    # Count leaf keys and track values across all patients
    all_keys = {}
    all_values = {}
    
    for patient_data in all_patient_data:
        # Extract keys and values from patient demographics
        demographics_keys, demographics_values = extract_leaf_keys(patient_data["demographics"], prefix="demographics")
        
        # Update key counts
        for key, count in demographics_keys.items():
            if key in all_keys:
                all_keys[key] += 1
            else:
                all_keys[key] = 1
        
        # Update value counts
        for key, values in demographics_values.items():
            if key not in all_values:
                all_values[key] = {}
            
            for value, count in values.items():
                if value in all_values[key]:
                    all_values[key][value] += count
                else:
                    all_values[key][value] = count
        
        # Extract keys and values from each resource type
        for resource_type, resources in patient_data["resources"].items():
            for resource in resources:
                resource_keys, resource_values = extract_leaf_keys(resource, prefix=f"resources.{resource_type}")
                
                # Update key counts
                for key, count in resource_keys.items():
                    if key in all_keys:
                        all_keys[key] += 1
                    else:
                        all_keys[key] = 1
                
                # Update value counts
                for key, values in resource_values.items():
                    if key not in all_values:
                        all_values[key] = {}
                    
                    for value, count in values.items():
                        if value in all_values[key]:
                            all_values[key][value] += count
                        else:
                            all_values[key][value] = count
    
    # Create result with key counts and top values
    result = {}
    for key, count in all_keys.items():
        # Get the top 3 most common values for this key
        top_values = ""
        if key in all_values:
            # Sort values by frequency (descending)
            sorted_values = sorted(all_values[key].items(), key=lambda item: item[1], reverse=True)
            
            # Take top 3
            top_3 = sorted_values[:3]
            
            if top_3:
                value_strings = [f"{value} - {count} occurrences" for value, count in top_3]
                top_values = "3 most common values: " + ", ".join(value_strings)
        
        # Add to result
        result[key] = {
            "count": count,
            "top_values": top_values
        }
    
    # Sort keys by frequency (descending)
    sorted_result = dict(sorted(result.items(), key=lambda item: item[1]["count"], reverse=True))
    
    return {
        "total_patients": len(all_patient_data),
        "cohort_id": cohort_id if cohort_id else "all",
        "key_analysis": sorted_result
    }


@app.delete("/delete-cohort/{cohort_id}", response_class=JSONResponse)
async def delete_cohort(cohort_id: str):
    """ Deletes a cohort from the HAPI FHIR server, including all patients with the cohort's tag.
    Args:
        cohort_id: The ID of the cohort to delete.
    Returns:
        A JSON object containing a message with the number of patients deleted.
    """
    hapi_url = "http://hapi:8080/fhir"
    
    # Check if the HAPI server is running
    try:
        r = requests.get(hapi_url + "/$meta")
        r.raise_for_status()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"HAPI FHIR server is not reachable. (It may be starting up.)"})
    
    # Try to fetch the Group resource
    group = fetch_group_by_id(hapi_url, cohort_id)
    if not group:
        return JSONResponse(status_code=404, content={"error": f"Cohort with ID '{cohort_id}' not found."})
    
    # Get patients from the group's member list
    group_patient_ids = []
    if "member" in group:
        group_patient_ids = [member.get("entity", {}).get("reference", "").replace("Patient/", "") 
                           for member in group["member"] if "entity" in member]
        group_patient_ids = set([pid for pid in group_patient_ids if pid])  # Remove empty IDs
    
    # Find all patients with this cohort tag
    tag_patient_ids = []
    try:
        # For FHIR search, we need to use the system|code format
        cohort_tag = f"urn:charm:cohort|{cohort_id}"
        
        # Get all patients with this cohort tag
        url = f"{hapi_url}/Patient?_tag={cohort_tag}&_count=5000"
        r = requests.get(url)
        r.raise_for_status()
        
        # Extract patient IDs from the search results
        tagged_patients = r.json()
        if "entry" in tagged_patients:
            for entry in tagged_patients["entry"]:
                if "resource" in entry and entry["resource"].get("resourceType") == "Patient":
                    patient_id = entry["resource"].get("id")
                    if patient_id and patient_id not in tag_patient_ids:
                        tag_patient_ids.append(patient_id)
    except Exception as e:
        logger.error(f"Error finding patients with cohort tag: {str(e)}")
    
    # Use only the tag-based patient IDs for deletion, as they're more reliable
    # The Group resource might contain references to patients that no longer exist
    # or that don't actually have the cohort tag
    patient_ids = tag_patient_ids
    
    # Log the counts for debugging
    logger.info(f"Cohort {cohort_id}: {len(group_patient_ids)} patients in group, {len(tag_patient_ids)} patients with tag")
    
    # Delete each patient
    deleted_count = 0
    failed_count = 0
    try:
        for patient_id in patient_ids:
            try:
                delete_url = f"{hapi_url}/Patient/{patient_id}"
                delete_r = requests.delete(delete_url)
                delete_r.raise_for_status()
                deleted_count += 1
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to delete patient {patient_id}: {str(e)}")
    
    except Exception as e:
        logger.error(f"Error deleting patients: {str(e)}")
        # Continue to delete the group even if patient deletion had issues
    
    # Delete the Group resource
    url = f"{hapi_url.rstrip('/')}/Group/{cohort_id}"
    try:
        r = requests.delete(url)
        r.raise_for_status()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error deleting cohort group: {str(e)}"})
    
    return {
        "message": f"Successfully deleted cohort '{cohort_id}' with {len(patient_ids)} patients ({deleted_count} deleted, {failed_count} failed).",
        "cohort_id": cohort_id,
        "patients_deleted": deleted_count,
        "patients_failed": failed_count,
        "total_patients": len(patient_ids)
    }

@app.post("/generate-download-synthetic-patients", response_class=JSONResponse)
async def generate_download_synthetic_patients(
    request: dict
):
    """Generates synthetic patients and returns them as a downloadable zip file.
    
    Args:
        num_patients: Number of synthetic patients to generate (default: 10).
        num_years: Number of years of history to generate for each patient (default: 1).
        exporter: Export format, either 'csv' or 'fhir' (default: 'fhir').
        min_age: Minimum age of generated patients (default: 0).
        max_age: Maximum age of generated patients (default: 140).
        gender: Gender of generated patients ('both', 'male', or 'female', default: 'both').
        
    Returns:
        A StreamingResponse with the zip file containing the generated patient data.
    """
    import os
    import tempfile
    import shutil
    import zipfile
    import asyncio

    # Extract parameters from request body
    num_patients = request.get('num_patients', 10)
    num_years = request.get('num_years', 1)
    exporter = request.get('exporter', 'fhir')
    min_age = request.get('min_age', 0)
    max_age = request.get('max_age', 140)
    gender = request.get('gender', 'both')
    
    # Print received parameters for debugging
    print(f"\n==== RECEIVED REQUEST PARAMETERS ====")
    print(f"num_patients: {num_patients}, type: {type(num_patients)}")
    print(f"num_years: {num_years}, type: {type(num_years)}")
    print(f"min_age: {min_age}, type: {type(min_age)}")
    print(f"max_age: {max_age}, type: {type(max_age)}")
    print(f"gender: {gender}, type: {type(gender)}")
    print(f"exporter: {exporter}, type: {type(exporter)}")
    print(f"==== END RECEIVED REQUEST PARAMETERS ====\n")
    
    # check if the exporter is valid
    if exporter not in ["csv", "fhir"]:
        return JSONResponse(status_code=400, content={"error": "Invalid exporter. Must be 'csv' or 'fhir'."})
    # check if the number of patients is valid
    if num_patients <= 0:
        return JSONResponse(status_code=400, content={"error": "Number of patients must be greater than 0."})
    # check if the number of years is valid
    if num_years <= 0:
        return JSONResponse(status_code=400, content={"error": "Number of years must be greater than 0."})
    
    async def generate_patient_data():
        try:
            # Use the existing run_synthea function that has the debug prints
            temp_dir, output_dir = run_synthea(
                num_patients=num_patients,
                num_years=num_years,
                min_age=min_age,
                max_age=max_age,
                gender=gender,
                exporter=exporter
            )
            
            # Create a zip file in memory
            zip_path = os.path.join(temp_dir, "synthea_output.zip")
            with zipfile.ZipFile(zip_path, 'w') as zf:
                # Add all generated files to the zip
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith(".csv") or file.endswith(".json") or file.endswith(".ndjson"):
                            file_path = os.path.join(root, file)
                            # Use relative path in the zip file
                            arc_name = os.path.relpath(file_path, temp_dir)
                            zf.write(file_path, arc_name)
            
            return zip_path
        except Exception as e:
            # Clean up the temp directory in case of error
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise e

    try:
        # Run synthea with a timeout
        loop = asyncio.get_event_loop()
        zip_path = await asyncio.wait_for(
            generate_patient_data(), 
            timeout=120
        )
        
        # Return the zip file as a download
        def iterfile():
            with open(zip_path, 'rb') as f:
                yield from f
            # Clean up after sending the file
            shutil.rmtree(os.path.dirname(zip_path), ignore_errors=True)
            
        response = StreamingResponse(iterfile(), media_type="application/zip")
        response.headers['Content-Disposition'] = f'attachment; filename="synthea_output.zip"'
        return response
    except subprocess.CalledProcessError as e:
        return JSONResponse(status_code=500, content={"error": f"Error running synthea: {e}"})
    except asyncio.TimeoutError:
        return JSONResponse(status_code=500, content={"error": "Error: Synthea took too long to run."})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error: {e}"})


@app.get("/download-cohort-zip/{cohort_id}")
def download_cohort_zip(cohort_id: int):
    """Downloads a zip file of a previously generated cohort.
    
    Args:
        cohort_id: The cohort number to download as zip.
        
    Returns:
        A StreamingResponse with the zip file or an error message.
    """
    import os
    import zipfile

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../syn_cohorts'))
    zip_path = os.path.join(base_dir, f"cohort-{cohort_id}.zip")
    cohort_dir = os.path.join(base_dir, str(cohort_id))

    if os.path.exists(zip_path):
        def iterfile():
            with open(zip_path, 'rb') as f:
                yield from f
        return StreamingResponse(iterfile(), media_type="application/zip", headers={
            'Content-Disposition': f'attachment; filename="cohort-{cohort_id}.zip"'
        })
    elif os.path.exists(cohort_dir) and os.path.isdir(cohort_dir):
        # Create zip file
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for root, dirs, files in os.walk(cohort_dir):
                for file in files:
                    if file.endswith(".csv") or file.endswith(".json"):
                        zf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), cohort_dir))
        def iterfile():
            with open(zip_path, 'rb') as f:
                yield from f
        return StreamingResponse(iterfile(), media_type="application/zip", headers={
            'Content-Disposition': f'attachment; filename="cohort-{cohort_id}.zip"'
        })
    else:
        return JSONResponse(status_code=404, content={"error": f"No zip or cohort folder found for cohort {cohort_id}"})


@app.get("/cohort-metadata/{cohort_id}")
def get_cohort_metadata(cohort_id: int):
    """Gets metadata for a previously generated cohort.
    
    Args:
        cohort_id: The cohort number to get metadata for.
        
    Returns:
        A JSONResponse with the cohort metadata or an error message.
    """
    import os
    import json
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../syn_cohorts'))
    cohort_dir = os.path.join(base_dir, str(cohort_id))
    metadata_dir = os.path.join(cohort_dir, "metadata")
    if not os.path.exists(cohort_dir) or not os.path.isdir(cohort_dir):
        return JSONResponse(status_code=404, content={"error": f"Cohort folder {cohort_id} not found."})
    if not os.path.exists(metadata_dir) or not os.path.isdir(metadata_dir):
        return JSONResponse(status_code=404, content={"error": f"Metadata folder not found for cohort {cohort_id}."})
    # Find the first .json file in the metadata directory
    json_files = [f for f in os.listdir(metadata_dir) if f.endswith('.json')]
    if not json_files:
        return JSONResponse(status_code=404, content={"error": f"No metadata JSON file found for cohort {cohort_id}."})
    metadata_path = os.path.join(metadata_dir, json_files[0])
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        return JSONResponse(status_code=200, content=metadata)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Could not read metadata: {str(e)}"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

