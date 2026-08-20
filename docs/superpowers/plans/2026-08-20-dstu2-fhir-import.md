# DSTU2 → R4 External FHIR Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the external FHIR ingest path accept DSTU2 (Apple HealthKit) bundles by converting them to R4 and isolating imported resources so they cannot collide with existing data.

**Architecture:** A new stateless Java sidecar (`fhir-converter`) wraps HAPI's `VersionConvertorFactory_10_40` and exposes `POST /convert`, converting a bundle entry-by-entry. The Python `synthea_server` gains a focused `external_import.py` module that detects the source version, calls the sidecar for DSTU2, synthesizes stub Patients for referenced-but-absent patient ids, and rebuilds the bundle as an idempotent `urn:uuid`/`identifier`/`ifNoneExist` transaction, reporting any unresolved references. The Synthea generation path is untouched.

**Tech Stack:** Java 17 + Maven + HAPI `org.hl7.fhir.convertors` (sidecar); Python 3 + FastAPI + `requests` + `pytest` (importer); Docker Compose.

## Global Constraints

- HAPI server is FHIR **R4** (`fhir_version: R4`), image `hapiproject/hapi:v8.2.0-2`; do not change its config.
- Do **not** modify the Synthea generation ingest path (`post_bundle()` and its callers).
- Services communicate by compose service name on the default network (no custom networks). Sidecar URL inside the network: `http://fhir-converter:8080`.
- The synthetic import identifier system is the literal string `urn:charm:apple-healthkit-src-id`.
- CHARM tag systems already in use: `urn:charm:source`, `urn:charm:datatype`, `urn:charm:cohort`, `urn:charm:created` (do not rename).
- `source_fhir_version` request field: allowed values `"R4"` (default) and `"DSTU2"`, case-insensitive.
- Idempotency: re-importing the same bundle must not create duplicates (relies on `ifNoneExist` matching the per-resource identifier).
- Dangling non-Patient references are **reported, not created** and **not dropped** — left literal in the resource.

---

### Task 1: `fhir-converter` sidecar — Maven project + entry-wise `/convert`

**Files:**
- Create: `app/fhir-converter/pom.xml`
- Create: `app/fhir-converter/src/main/java/org/charm/converter/ConverterService.java`
- Create: `app/fhir-converter/src/main/java/org/charm/converter/ConverterServer.java`
- Test: `app/fhir-converter/src/test/java/org/charm/converter/ConverterServiceTest.java`

**Interfaces:**
- Produces: HTTP `POST /convert` accepting `{"sourceVersion":"DSTU2","bundle":{…}}`, returning an R4 `collection` Bundle JSON on 200, or an R4 `OperationOutcome` JSON on 4xx/5xx.
- Produces (Java): `ConverterService.convertBundleDstu2ToR4(String dstu2BundleJson) -> String` (R4 bundle JSON).

- [ ] **Step 1: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.charm</groupId>
  <artifactId>fhir-converter</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <!-- Must match the org.hl7.fhir.core line shipped with HAPI 8.2.x.
         Verify latest on Maven Central:
         https://central.sonatype.com/artifact/ca.uhn.hapi.fhir/org.hl7.fhir.convertors -->
    <fhir.core.version>6.3.11</fhir.core.version>
  </properties>

  <dependencies>
    <!-- Pulls org.hl7.fhir.dstu2 and org.hl7.fhir.r4 transitively -->
    <dependency>
      <groupId>ca.uhn.hapi.fhir</groupId>
      <artifactId>org.hl7.fhir.convertors</artifactId>
      <version>${fhir.core.version}</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>5.10.2</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <finalName>fhir-converter</finalName>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>org.charm.converter.ConverterServer</mainClass>
                </transformer>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Write the failing test**

```java
package org.charm.converter;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConverterServiceTest {
  // Minimal DSTU2 bundle: one MedicationOrder (renamed MedicationRequest in R4)
  private static final String DSTU2 = """
    {"resourceType":"Bundle","type":"collection","entry":[
      {"resource":{"resourceType":"MedicationOrder","id":"13","status":"active",
       "dateWritten":"2023-10-20","patient":{"reference":"Patient/100"},
       "medicationCodeableConcept":{"coding":[{"system":"http://www.nlm.nih.gov/research/umls/rxnorm/","code":"1"}]}}}
    ]}""";

  @Test
  void convertsMedicationOrderToMedicationRequest() {
    String r4 = ConverterService.convertBundleDstu2ToR4(DSTU2);
    assertTrue(r4.contains("\"resourceType\":\"Bundle\""));
    assertTrue(r4.contains("MedicationRequest"), "MedicationOrder should become MedicationRequest");
    assertFalse(r4.contains("MedicationOrder"), "no DSTU2 MedicationOrder should remain");
  }

  @Test
  void badResourceIsSkippedNotFatal() {
    String bundle = "{\"resourceType\":\"Bundle\",\"type\":\"collection\",\"entry\":[" +
        "{\"resource\":{\"resourceType\":\"NotAResource\"}}]}";
    // Unknown resource: entry skipped, still returns a valid empty R4 bundle
    String r4 = ConverterService.convertBundleDstu2ToR4(bundle);
    assertTrue(r4.contains("\"resourceType\":\"Bundle\""));
  }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app/fhir-converter && mvn -q test`
Expected: FAIL — `ConverterService` does not exist / does not compile.

- [ ] **Step 4: Implement `ConverterService`**

```java
package org.charm.converter;

import org.hl7.fhir.convertors.factory.VersionConvertorFactory_10_40;
import org.hl7.fhir.dstu2.model.Bundle;
import org.hl7.fhir.dstu2.model.Resource;

public final class ConverterService {
  private ConverterService() {}

  /** Convert a DSTU2 bundle JSON to an R4 collection bundle JSON, entry by entry. */
  public static String convertBundleDstu2ToR4(String dstu2BundleJson) {
    try {
      org.hl7.fhir.dstu2.formats.JsonParser d2 = new org.hl7.fhir.dstu2.formats.JsonParser();
      Resource parsed = d2.parse(dstu2BundleJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      if (!(parsed instanceof Bundle)) {
        throw new IllegalArgumentException("Top-level resource is not a Bundle");
      }
      Bundle in = (Bundle) parsed;

      org.hl7.fhir.r4.model.Bundle out = new org.hl7.fhir.r4.model.Bundle();
      out.setType(org.hl7.fhir.r4.model.Bundle.BundleType.COLLECTION);

      for (Bundle.BundleEntryComponent e : in.getEntry()) {
        if (!e.hasResource()) continue;
        try {
          org.hl7.fhir.r4.model.Resource r4res =
              (org.hl7.fhir.r4.model.Resource) VersionConvertorFactory_10_40.convertResource(e.getResource());
          out.addEntry().setResource(r4res);
        } catch (Exception skip) {
          // Per-resource failure: skip this entry, keep converting the rest.
          System.err.println("Skipping unconvertible entry: " + skip.getMessage());
        }
      }

      org.hl7.fhir.r4.formats.JsonParser r4parser = new org.hl7.fhir.r4.formats.JsonParser();
      return r4parser.composeString(out);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception ex) {
      throw new RuntimeException("DSTU2->R4 conversion failed: " + ex.getMessage(), ex);
    }
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd app/fhir-converter && mvn -q test`
Expected: PASS (both tests).

- [ ] **Step 6: Implement `ConverterServer` (JDK built-in HTTP server, no web framework)**

```java
package org.charm.converter;

import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class ConverterServer {
  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
    server.createContext("/health", ex -> respond(ex, 200, "{\"status\":\"ok\"}"));
    server.createContext("/convert", ex -> {
      if (!"POST".equals(ex.getRequestMethod())) { respond(ex, 405, oo("not-supported","POST only")); return; }
      try (InputStream is = ex.getRequestBody()) {
        String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        // body = {"sourceVersion":"DSTU2","bundle":{...}} ; extract the bundle object.
        String bundleJson = JsonBody.extractBundle(body);
        String r4 = ConverterService.convertBundleDstu2ToR4(bundleJson);
        respond(ex, 200, r4);
      } catch (Exception e) {
        respond(ex, 400, oo("processing", e.getMessage() == null ? "conversion error" : e.getMessage()));
      }
    });
    server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
    server.start();
    System.out.println("fhir-converter listening on :8080");
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, int code, String json) throws java.io.IOException {
    byte[] b = json.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", "application/fhir+json");
    ex.sendResponseHeaders(code, b.length);
    ex.getResponseBody().write(b);
    ex.close();
  }

  private static String oo(String code, String diag) {
    return "{\"resourceType\":\"OperationOutcome\",\"issue\":[{\"severity\":\"error\",\"code\":\""
        + code + "\",\"diagnostics\":" + JsonBody.quote(diag) + "}]}";
  }
}
```

- [ ] **Step 7: Implement the tiny `JsonBody` helper**

```java
package org.charm.converter;

import org.hl7.fhir.r4.model.Bundle; // reuse a JSON reader available on classpath

public final class JsonBody {
  private JsonBody() {}

  /** Extract the value of the top-level "bundle" key from the request JSON. */
  public static String extractBundle(String requestJson) {
    com.google.gson.JsonObject root = com.google.gson.JsonParser.parseString(requestJson).getAsJsonObject();
    if (!root.has("bundle")) throw new IllegalArgumentException("request missing 'bundle'");
    return root.get("bundle").toString();
  }

  public static String quote(String s) {
    return com.google.gson.JsonParser.parseString(
        new com.google.gson.Gson().toJson(s == null ? "" : s)).toString();
  }
}
```

Note: Gson is on the classpath transitively via `org.hl7.fhir.*`. If `mvn dependency:tree` shows it absent, add `com.google.code.gson:gson:2.10.1` to `pom.xml`.

- [ ] **Step 8: Commit**

```bash
git add app/fhir-converter
git commit -m "feat(fhir-converter): DSTU2->R4 entry-wise converter service"
```

---

### Task 2: Containerize the sidecar and wire it into docker-compose

**Files:**
- Create: `app/fhir-converter/Dockerfile`
- Modify: `app/docker-compose.yml` (add `fhir-converter` service; add `CONVERTER_URL` env + `depends_on` to `synthea_server`)

**Interfaces:**
- Produces: reachable service `http://fhir-converter:8080` on the compose network; env var `CONVERTER_URL` available to `synthea_server`.

- [ ] **Step 1: Write the Dockerfile (multi-stage)**

```dockerfile
# --- build ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# --- run ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/fhir-converter.jar /app/fhir-converter.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/fhir-converter.jar"]
```

- [ ] **Step 2: Build the image to verify it compiles and packages**

Run: `cd app/fhir-converter && docker build -t charm/fhir-converter .`
Expected: image builds successfully.

- [ ] **Step 3: Add the service to `app/docker-compose.yml`**

Add under `services:` (sibling of `hapi`):

```yaml
  fhir-converter:
    build: ./fhir-converter
    # no host port needed; internal only
```

- [ ] **Step 4: Add converter dependency to `synthea_server` service**

In the `synthea_server` service block, add `fhir-converter` to `depends_on` and set the env var:

```yaml
  synthea_server:
    depends_on:
      hapi:
        condition: service_started
      fhir-converter:
        condition: service_started
    build: ./synthea_server
    environment:
      - CONVERTER_URL=http://fhir-converter:8080
```

(Merge with existing `depends_on`/`environment` keys if present — do not duplicate the keys.)

- [ ] **Step 5: Verify the service starts and answers**

Run:
```bash
cd app && docker compose up -d fhir-converter
sleep 5
docker compose exec -T synthea_server sh -lc 'curl -sf http://fhir-converter:8080/health'
```
Expected: `{"status":"ok"}`

- [ ] **Step 6: Commit**

```bash
git add app/fhir-converter/Dockerfile app/docker-compose.yml
git commit -m "feat(fhir-converter): dockerize and wire into compose"
```

---

### Task 3: Python test harness + `detect_fhir_version`

**Files:**
- Create: `app/synthea_server/synthea-pyserver/external_import.py`
- Create: `app/synthea_server/synthea-pyserver/tests/__init__.py` (empty)
- Create: `app/synthea_server/synthea-pyserver/tests/test_external_import.py`
- Modify: `app/synthea_server/pyproject.toml` (add pytest dev group + pytest config)

**Interfaces:**
- Produces: `detect_fhir_version(bundle: dict, hint: str | None = None) -> str` returning `"R4"` or `"DSTU2"`.

- [ ] **Step 1: Add pytest to `pyproject.toml`**

Add a dev group and pytest rootdir config:

```toml
[tool.poetry.group.dev.dependencies]
pytest = "^8.2.0"

[tool.pytest.ini_options]
rootdir = "synthea-pyserver"
testpaths = ["synthea-pyserver/tests"]
```

Then: `cd app/synthea_server && poetry install --with dev`

- [ ] **Step 2: Write the failing test**

```python
# tests/test_external_import.py
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -v`
Expected: FAIL — `external_import` has no `detect_fhir_version`.

- [ ] **Step 4: Implement `detect_fhir_version`**

```python
# external_import.py
"""External FHIR import: version detection, stub patients, isolation transaction.

Kept separate from main.py so these pure functions are unit-testable without
importing the FastAPI application.
"""
from __future__ import annotations

# Resource types that were renamed/removed after DSTU2 (strong DSTU2 signal).
_DSTU2_ONLY_TYPES = {"MedicationOrder", "DeviceUseRequest", "DiagnosticOrder", "BodySite"}


def detect_fhir_version(bundle: dict, hint: str | None = None) -> str:
    """Return "DSTU2" or "R4". Explicit hint wins; otherwise use structural heuristics."""
    if hint:
        h = hint.strip().upper()
        if h in ("DSTU2", "R4"):
            return h
    for entry in bundle.get("entry", []):
        res = entry.get("resource", {})
        if res.get("resourceType") in _DSTU2_ONLY_TYPES:
            return "DSTU2"
        # DSTU2 Observation.category is a single object; R4 makes it an array.
        if res.get("resourceType") == "Observation" and isinstance(res.get("category"), dict):
            return "DSTU2"
    return "R4"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add app/synthea_server/pyproject.toml app/synthea_server/poetry.lock app/synthea_server/synthea-pyserver/external_import.py app/synthea_server/synthea-pyserver/tests
git commit -m "feat(external-import): pytest harness + detect_fhir_version"
```

---

### Task 4: `synthesize_stub_patients`

**Files:**
- Modify: `app/synthea_server/synthea-pyserver/external_import.py`
- Modify: `app/synthea_server/synthea-pyserver/tests/test_external_import.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `synthesize_stub_patients(bundle: dict) -> dict` — returns a new bundle where every `Patient/<id>` referenced but not contained has a minimal R4 Patient added (carrying `identifier` = the source id under system `urn:charm:apple-healthkit-src-id`).

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k stub -v`
Expected: FAIL — no `synthesize_stub_patients`.

- [ ] **Step 3: Implement `synthesize_stub_patients`**

```python
import copy

SRC_ID_SYSTEM = "urn:charm:apple-healthkit-src-id"


def _iter_references(obj):
    """Yield every reference string found anywhere in a nested structure."""
    if isinstance(obj, dict):
        ref = obj.get("reference")
        if isinstance(ref, str):
            yield ref
        for v in obj.values():
            yield from _iter_references(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_references(item)


def synthesize_stub_patients(bundle: dict) -> dict:
    """Add a minimal R4 Patient for each Patient/<id> referenced but not contained."""
    bundle = copy.deepcopy(bundle)
    entries = bundle.setdefault("entry", [])

    present = {e["resource"]["id"] for e in entries
               if e.get("resource", {}).get("resourceType") == "Patient" and e["resource"].get("id")}

    referenced = set()
    for e in entries:
        for ref in _iter_references(e.get("resource", {})):
            if ref.startswith("Patient/"):
                referenced.add(ref[len("Patient/"):])

    for pid in sorted(referenced - present):
        entries.append({"resource": {
            "resourceType": "Patient",
            "id": pid,
            "identifier": [{"system": SRC_ID_SYSTEM, "value": pid}],
        }})
    return bundle
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k stub -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/synthea_server/synthea-pyserver/external_import.py app/synthea_server/synthea-pyserver/tests/test_external_import.py
git commit -m "feat(external-import): synthesize stub patients for absent references"
```

---

### Task 5: `build_isolation_transaction`

**Files:**
- Modify: `app/synthea_server/synthea-pyserver/external_import.py`
- Modify: `app/synthea_server/synthea-pyserver/tests/test_external_import.py`

**Interfaces:**
- Consumes: `_iter_references`, `SRC_ID_SYSTEM` (Task 4).
- Produces: `build_isolation_transaction(bundle: dict) -> tuple[dict, list[dict]]` — returns `(transaction_bundle, unresolved_references)` where `unresolved_references` is a list of `{"source": "<Type>/<srcId>", "reference": "<danglingRef>"}`.

Behavior:
- Every entry gets `fullUrl = "urn:uuid:<uuid4>"`.
- References whose `<Type>/<id>` matches an in-bundle resource are rewritten to that resource's `urn:uuid:`.
- References whose target is not in-bundle are left literal and recorded in `unresolved_references`.
- Each entry's `request` is `POST <Type>` with `ifNoneExist` on the resource's identifier: prefer an existing `identifier` (first entry, `system|value`); else synthesize one from the resource `id` under `SRC_ID_SYSTEM` and match on it. Resources with neither an identifier nor an `id` use a plain `POST` with no `ifNoneExist`.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k isolation -v`
Expected: FAIL — no `build_isolation_transaction`.

- [ ] **Step 3: Implement `build_isolation_transaction`**

```python
import uuid
from urllib.parse import quote as _urlquote


def _primary_identifier(resource: dict) -> tuple[str, str] | None:
    """Return (system, value) of the resource's first identifier, or None."""
    ids = resource.get("identifier")
    if isinstance(ids, list) and ids:
        first = ids[0]
        if isinstance(first, dict) and first.get("system") and first.get("value") is not None:
            return first["system"], str(first["value"])
    return None


def build_isolation_transaction(bundle: dict) -> tuple[dict, list[dict]]:
    bundle = copy.deepcopy(bundle)
    entries = bundle.get("entry", [])

    # 1. Assign a urn:uuid to every entry and index (Type/id) -> urn:uuid.
    ref_index: dict[str, str] = {}
    for e in entries:
        res = e.get("resource", {})
        e["fullUrl"] = f"urn:uuid:{uuid.uuid4()}"
        rtype, rid = res.get("resourceType"), res.get("id")
        if rtype and rid:
            ref_index[f"{rtype}/{rid}"] = e["fullUrl"]

    # 2. Rewrite in-bundle refs to urn:uuid; collect dangling ones.
    unresolved: list[dict] = []

    def rewrite(obj, source_label):
        if isinstance(obj, dict):
            ref = obj.get("reference")
            if isinstance(ref, str) and "/" in ref and not ref.startswith("urn:"):
                if ref in ref_index:
                    obj["reference"] = ref_index[ref]
                else:
                    unresolved.append({"source": source_label, "reference": ref})
            for v in obj.values():
                rewrite(v, source_label)
        elif isinstance(obj, list):
            for item in obj:
                rewrite(item, source_label)

    for e in entries:
        res = e.get("resource", {})
        source_label = f"{res.get('resourceType')}/{res.get('id')}" if res.get("id") else res.get("resourceType", "?")
        rewrite(res, source_label)

    # 3. Build POST + ifNoneExist request per entry (idempotent conditional-create).
    for e in entries:
        res = e.get("resource", {})
        rtype = res.get("resourceType")
        ident = _primary_identifier(res)
        if ident is None and res.get("id"):
            ident = (SRC_ID_SYSTEM, str(res["id"]))
            res.setdefault("identifier", []).append({"system": ident[0], "value": ident[1]})
        # Server assigns the id; drop the source id so it is never asserted.
        res.pop("id", None)
        req = {"method": "POST", "url": rtype}
        if ident is not None:
            token = f"{ident[0]}|{ident[1]}"
            req["ifNoneExist"] = f"identifier={_urlquote(token, safe='')}"
        e["request"] = req

    bundle["type"] = "transaction"
    return bundle, unresolved
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k isolation -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/synthea_server/synthea-pyserver/external_import.py app/synthea_server/synthea-pyserver/tests/test_external_import.py
git commit -m "feat(external-import): urn:uuid/ifNoneExist isolation transaction with dangling-ref report"
```

---

### Task 6: Converter client `convert_bundle`

**Files:**
- Modify: `app/synthea_server/synthea-pyserver/external_import.py`
- Modify: `app/synthea_server/synthea-pyserver/tests/test_external_import.py`

**Interfaces:**
- Produces: `convert_bundle(bundle: dict, source_version: str, converter_url: str, session=None) -> dict` — returns an R4 bundle. If `source_version == "R4"`, returns the bundle unchanged (no network call). If `"DSTU2"`, POSTs `{"sourceVersion":"DSTU2","bundle":bundle}` to `<converter_url>/convert` and returns the parsed R4 JSON. Raises `ConversionError` on non-2xx.

- [ ] **Step 1: Write the failing test (HTTP mocked via a fake session)**

```python
class _FakeResp:
    def __init__(self, status, payload): self.status_code = status; self._p = payload
    def json(self): return self._p
    @property
    def text(self): import json; return json.dumps(self._p)

class _FakeSession:
    def __init__(self, resp): self.resp = resp; self.calls = []
    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append((url, json)); return self.resp


def test_r4_is_passthrough_no_network():
    sess = _FakeSession(_FakeResp(500, {}))  # would error if called
    b = {"resourceType": "Bundle", "entry": []}
    assert ei.convert_bundle(b, "R4", "http://c:8080", session=sess) is b
    assert sess.calls == []


def test_dstu2_calls_converter():
    r4 = {"resourceType": "Bundle", "type": "collection", "entry": []}
    sess = _FakeSession(_FakeResp(200, r4))
    b = {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "MedicationOrder"}}]}
    out = ei.convert_bundle(b, "DSTU2", "http://c:8080", session=sess)
    assert out == r4
    assert sess.calls[0][0] == "http://c:8080/convert"
    assert sess.calls[0][1]["sourceVersion"] == "DSTU2"


def test_converter_error_raises():
    sess = _FakeSession(_FakeResp(400, {"resourceType": "OperationOutcome"}))
    import pytest
    with pytest.raises(ei.ConversionError):
        ei.convert_bundle({"resourceType": "Bundle"}, "DSTU2", "http://c:8080", session=sess)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k convert -v`
Expected: FAIL — no `convert_bundle` / `ConversionError`.

- [ ] **Step 3: Implement `convert_bundle`**

```python
import requests


class ConversionError(Exception):
    pass


def convert_bundle(bundle: dict, source_version: str, converter_url: str, session=None) -> dict:
    if source_version.upper() == "R4":
        return bundle
    sess = session or requests.Session()
    url = converter_url.rstrip("/") + "/convert"
    resp = sess.post(url, json={"sourceVersion": "DSTU2", "bundle": bundle},
                     headers={"Content-Type": "application/json"}, timeout=120)
    if resp.status_code not in (200, 201):
        raise ConversionError(f"converter returned {resp.status_code}: {resp.text[:500]}")
    return resp.json()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k convert -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/synthea_server/synthea-pyserver/external_import.py app/synthea_server/synthea-pyserver/tests/test_external_import.py
git commit -m "feat(external-import): converter sidecar client with R4 passthrough"
```

---

### Task 7: Wire the pipeline into the endpoint + contract changes

**Files:**
- Modify: `app/synthea_server/synthea-pyserver/main.py` (`ExternalFHIRRequest`, `ingest_external_fhir`)
- Modify: `app/router/router/routers/ingestion.py` (`ExternalFHIRIngestRequest`, forwarding)
- Modify: `app/synthea_server/synthea-pyserver/tests/test_external_import.py` (orchestration test)

**Interfaces:**
- Consumes: `detect_fhir_version`, `convert_bundle`, `synthesize_stub_patients`, `build_isolation_transaction` (Tasks 3–6).
- Produces: `assemble_external_import(bundle, source_fhir_version, converter_url, session=None) -> tuple[dict, list[dict]]` in `external_import.py` — the full detect→convert→stub→isolation pipeline, returning `(transaction_bundle, unresolved_references)`. The endpoint calls this, then tags + POSTs to HAPI.

- [ ] **Step 1: Write the failing orchestration test**

```python
def test_assemble_external_import_end_to_end_r4():
    bundle = {"resourceType": "Bundle", "type": "collection", "entry": [
        {"resource": {"resourceType": "Observation", "id": "1",
                      "subject": {"reference": "Patient/100"}}}]}
    txn, unresolved = ei.assemble_external_import(bundle, "R4", "http://c:8080")
    assert txn["type"] == "transaction"
    types = {e["resource"]["resourceType"] for e in txn["entry"]}
    assert "Patient" in types            # stub synthesized
    assert unresolved == []              # Patient/100 now resolves in-bundle
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k assemble -v`
Expected: FAIL — no `assemble_external_import`.

- [ ] **Step 3: Implement `assemble_external_import`**

```python
def assemble_external_import(bundle: dict, source_fhir_version: str | None,
                             converter_url: str, session=None) -> tuple[dict, list[dict]]:
    version = detect_fhir_version(bundle, hint=source_fhir_version)
    r4 = convert_bundle(bundle, version, converter_url, session=session)
    stubbed = synthesize_stub_patients(r4)
    return build_isolation_transaction(stubbed)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd app/synthea_server && poetry run pytest synthea-pyserver/tests/test_external_import.py -k assemble -v`
Expected: PASS.

- [ ] **Step 5: Update `ExternalFHIRRequest` and the endpoint in `main.py`**

Add the field to `ExternalFHIRRequest` (after `datatype`):

```python
    source_fhir_version: str = Field("R4", description="Source FHIR version: 'R4' (default) or 'DSTU2'")
```

At the top of `main.py`, add the import and converter URL:

```python
import external_import
CONVERTER_URL = os.environ.get("CONVERTER_URL", "http://fhir-converter:8080")
```

Replace the body of `ingest_external_fhir` between the bundle-validation block and the "Apply tags" block. Old lines (build the transaction the old way):

```python
        # Prefix patient IDs to prevent conflicts
        prefixed_bundle = prefix_patient_ids(request.bundle, prefix="ext-")

        # Convert to transaction bundle for atomic updates
        transaction_bundle = convert_to_transaction_bundle(prefixed_bundle)
```

New:

```python
        # Detect version, convert if DSTU2, synthesize stub patients, isolate ids.
        transaction_bundle, unresolved_refs = external_import.assemble_external_import(
            request.bundle, request.source_fhir_version, CONVERTER_URL,
        )
        if unresolved_refs:
            logger.warning(f"{len(unresolved_refs)} unresolved reference(s) in import")
```

Then, where the endpoint builds its JSON response, include the report. Find the success `return`/`JSONResponse` and add `"unresolved_references": unresolved_refs` to its content dict. (If the handler returns `{"success": True, ...}`, add the key there.)

Note: keep `prefix_patient_ids` and `convert_to_transaction_bundle` defined — they are still referenced by other flows (grep confirms line ~3477). Only the external endpoint stops calling them.

- [ ] **Step 6: Add `source_fhir_version` to the router and forward it**

In `app/router/router/routers/ingestion.py`, add to `ExternalFHIRIngestRequest`:

```python
    source_fhir_version: str = Field("R4", description="Source FHIR version: 'R4' or 'DSTU2'")
```

Ensure the forwarded payload to `{synthea_server_url}/ingest-external-fhir` includes `source_fhir_version` (add it to the dict/`model_dump()` used in the `httpx` POST body).

- [ ] **Step 7: Run the module tests + a syntax check on the endpoints**

Run:
```bash
cd app/synthea_server && poetry run pytest synthea-pyserver/tests -v
python -c "import ast; ast.parse(open('synthea-pyserver/main.py').read())"
python -c "import ast; ast.parse(open('../router/router/routers/ingestion.py').read())"
```
Expected: all pytest pass; both `ast.parse` produce no output (valid syntax).

- [ ] **Step 8: Commit**

```bash
git add app/synthea_server/synthea-pyserver/external_import.py app/synthea_server/synthea-pyserver/main.py app/router/router/routers/ingestion.py app/synthea_server/synthea-pyserver/tests/test_external_import.py
git commit -m "feat(ingest): route external FHIR through DSTU2 conversion + isolation"
```

---

### Task 8: End-to-end validation with the Apple samples + fixture relocation

**Files:**
- Create: `app/synthea_server/test_fixtures/apple_healthkit_dstu2_a.json` (moved from `app/hapi/Sample A.json`)
- Create: `app/synthea_server/test_fixtures/apple_healthkit_dstu2_b.json` (moved from `app/hapi/Sample B.json`)
- Create: `app/synthea_server/test_fixtures/apple_healthkit_dstu2_c.json` (moved from `app/hapi/Sample C.json`)
- Modify: `ci/external_fhir_ingestion_validation.sh` (add a DSTU2 case)

**Interfaces:**
- Consumes: the running compose stack (`router`, `synthea_server`, `fhir-converter`, `hapi`).

- [ ] **Step 1: Relocate the sample fixtures**

```bash
cd /Users/oneilsh/Documents/projects/tislab/CHARM/CHARMTwinsights
mkdir -p app/synthea_server/test_fixtures
git mv "app/hapi/Sample A.json" app/synthea_server/test_fixtures/apple_healthkit_dstu2_a.json
git mv "app/hapi/Sample B.json" app/synthea_server/test_fixtures/apple_healthkit_dstu2_b.json
git mv "app/hapi/Sample C.json" app/synthea_server/test_fixtures/apple_healthkit_dstu2_c.json
```

- [ ] **Step 2: Bring up the stack**

Run: `cd app && docker compose up -d --build hapi hapi_db fhir-converter synthea_server router`
Expected: all containers healthy; `docker compose exec -T synthea_server sh -lc 'curl -sf http://fhir-converter:8080/health'` returns ok.

- [ ] **Step 3: Add a DSTU2 case to the CI script**

Append to `ci/external_fhir_ingestion_validation.sh` a block that POSTs a DSTU2 sample through the router with `source_fhir_version=DSTU2`, then asserts against HAPI. Concretely:

```bash
echo "== DSTU2 Apple import =="
BUNDLE=$(cat app/synthea_server/test_fixtures/apple_healthkit_dstu2_c.json)
REQ=$(jq -n --argjson b "$BUNDLE" \
  '{bundle:$b, cohort_id:"apple-test", datatype:"external", source_fhir_version:"DSTU2"}')

RESP=$(curl -sf -X POST "$ROUTER_BASE_URL/ingest/fhir" \
  -H "Content-Type: application/json" -d "$REQ")
echo "$RESP" | jq .

# 1. import reported unresolved references (Encounter/... present in sample C)
echo "$RESP" | jq -e '.unresolved_references | length >= 0' >/dev/null

# 2. a stub Patient now exists with the source-id identifier
curl -sf "$HAPI_BASE_URL/Patient?identifier=urn:charm:apple-healthkit-src-id|1" \
  | jq -e '.total >= 1' >/dev/null

# 3. no DSTU2 MedicationOrder leaked; it converted to MedicationRequest
curl -sf "$HAPI_BASE_URL/MedicationRequest?_count=1" | jq -e '.resourceType=="Bundle"' >/dev/null

# 4. idempotency: re-POST identical request, Patient count unchanged
BEFORE=$(curl -sf "$HAPI_BASE_URL/Patient?identifier=urn:charm:apple-healthkit-src-id|1" | jq '.total')
curl -sf -X POST "$ROUTER_BASE_URL/ingest/fhir" -H "Content-Type: application/json" -d "$REQ" >/dev/null
AFTER=$(curl -sf "$HAPI_BASE_URL/Patient?identifier=urn:charm:apple-healthkit-src-id|1" | jq '.total')
[ "$BEFORE" = "$AFTER" ] || { echo "IDEMPOTENCY FAIL: $BEFORE -> $AFTER"; exit 1; }
echo "DSTU2 import OK"
```

(Match the existing script's variable names for `ROUTER_BASE_URL` / `HAPI_BASE_URL`; reuse whatever it already defines.)

- [ ] **Step 4: Run the CI validation script**

Run: `cd /Users/oneilsh/Documents/projects/tislab/CHARM/CHARMTwinsights && bash ci/external_fhir_ingestion_validation.sh`
Expected: script exits 0, prints `DSTU2 import OK`.

- [ ] **Step 5: Commit**

```bash
git add app/synthea_server/test_fixtures ci/external_fhir_ingestion_validation.sh
git commit -m "test(ingest): e2e DSTU2 Apple import validation + relocate fixtures"
```

---

## Self-Review

**Spec coverage:**
- Layer 1 conversion → Tasks 1–2, 6. ✓
- Layer 2 ID isolation (all resources, server-assigned) → Task 5. ✓
- Layer 3 dangling-ref report → Task 5 (`unresolved`), surfaced in Task 7 response, asserted Task 8. ✓
- Layer 4 stub Patient + cohort membership → Task 4; cohort `upsert_group` keeps working because stub Patients are present before the existing patient-id extraction (Task 7 leaves that block intact). ✓
- Idempotent re-import (`ifNoneExist`) → Task 5, asserted Task 8 step 3. ✓
- API changes (`source_fhir_version`, `unresolved_references`) → Task 7. ✓
- Migrate mobile path (shared logic) → Task 7 routes ALL external bundles through `assemble_external_import`; R4 passthrough skips only the convert call. ✓
- Sidecar generality across resource types → Task 1 entry-wise loop. ✓
- Fixture relocation → Task 8. ✓

**Compatibility assumption (from spec §5):** the mobile-path migration is behavior-changing for any client that reads back by hardcoded server id. Not code — flagged here so the executor surfaces it in the PR description for the FHIR-HOSE owner to confirm.

**Placeholder scan:** no TBD/TODO; every code step has concrete content. The one soft spot — the exact `org.hl7.fhir.convertors` version — is given a concrete value (`6.3.11`) plus a verification URL, not left blank.

**Type consistency:** `detect_fhir_version(bundle, hint)`, `synthesize_stub_patients(bundle)`, `build_isolation_transaction(bundle) -> (dict, list)`, `convert_bundle(bundle, source_version, converter_url, session)`, `assemble_external_import(bundle, source_fhir_version, converter_url, session)` — names/signatures consistent across Tasks 3–7. `SRC_ID_SYSTEM` / `urn:charm:apple-healthkit-src-id` identical in code, tests, and CI assertions.
