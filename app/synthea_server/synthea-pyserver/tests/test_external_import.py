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
