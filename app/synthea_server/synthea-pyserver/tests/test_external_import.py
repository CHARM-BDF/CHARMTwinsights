import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import external_import as ei


def test_explicit_hint_wins():
    assert ei.detect_fhir_version({"resourceType": "Bundle", "entry": []}, hint="dstu2") == "DSTU2"
    assert ei.detect_fhir_version({"resourceType": "Bundle", "entry": []}, hint="R4") == "R4"


def test_heuristic_detects_dstu2_medicationorder():
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "MedicationOrder", "id": "1"}}]}
    assert ei.detect_fhir_version(bundle) == "DSTU2"


def test_heuristic_detects_dstu2_category_object():
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Observation",
                      "category": {"coding": [{"code": "laboratory"}]}}}]}
    assert ei.detect_fhir_version(bundle) == "DSTU2"


def test_heuristic_defaults_r4():
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Observation", "category": [{"coding": []}]}}]}
    assert ei.detect_fhir_version(bundle) == "R4"


def test_synthesizes_missing_patient():
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"resource": {"resourceType": "Observation", "id": "1",
                      "subject": {"reference": "Patient/100"}}}]}
    out = ei.synthesize_stub_patients(bundle)
    patients = [e["resource"] for e in out["entry"]
                if e["resource"]["resourceType"] == "Patient"]
    assert len(patients) == 1
    assert patients[0]["id"] == "100"
    assert patients[0]["identifier"][0]["system"] == "urn:charm:apple-healthkit-src-id"
    assert patients[0]["identifier"][0]["value"] == "100"


def test_does_not_duplicate_existing_patient():
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"resource": {"resourceType": "Patient", "id": "100"}},
        {"resource": {"resourceType": "Observation", "id": "1",
                      "subject": {"reference": "Patient/100"}}}]}
    out = ei.synthesize_stub_patients(bundle)
    patients = [e["resource"] for e in out["entry"]
                if e["resource"]["resourceType"] == "Patient"]
    assert len(patients) == 1


def test_isolation_rewrites_in_bundle_refs_and_reports_dangling():
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"resource": {"resourceType": "Patient", "id": "100",
                      "identifier": [{"system": "urn:charm:apple-healthkit-src-id", "value": "100"}]}},
        {"resource": {"resourceType": "Observation", "id": "1",
                      "subject": {"reference": "Patient/100"},
                      "encounter": {"reference": "Encounter/355"}}},
    ]}
    txn, unresolved = ei.build_isolation_transaction(bundle)

    assert txn["type"] == "transaction"
    # every entry has a urn:uuid fullUrl and a POST request
    for e in txn["entry"]:
        assert e["fullUrl"].startswith("urn:uuid:")
        assert e["request"]["method"] == "POST"
        assert "ifNoneExist" in e["request"]

    # the in-bundle Patient ref was rewritten to the Patient entry's urn:uuid
    patient_uuid = next(e["fullUrl"] for e in txn["entry"]
                        if e["resource"]["resourceType"] == "Patient")
    obs = next(e["resource"] for e in txn["entry"]
               if e["resource"]["resourceType"] == "Observation")
    assert obs["subject"]["reference"] == patient_uuid

    # the dangling Encounter ref is untouched and reported
    assert obs["encounter"]["reference"] == "Encounter/355"
    assert {"source": "Observation/1", "reference": "Encounter/355"} in unresolved


def test_isolation_synthesizes_identifier_when_absent():
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"resource": {"resourceType": "Condition", "id": "7"}}]}
    txn, _ = ei.build_isolation_transaction(bundle)
    req = txn["entry"][0]["request"]
    assert req["method"] == "POST"
    assert req["url"] == "Condition"
    assert "identifier=urn:charm:apple-healthkit-src-id%7C7" in req["ifNoneExist"]
