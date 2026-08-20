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

  @Test
  void mixedBundleSkipsBadEntryButKeepsGoodEntry() {
    // One good MedicationOrder entry alongside one bad/unknown-resource-type entry.
    // Asserts that the bad entry is dropped WHILE the good entry survives -- this
    // is what distinguishes "skip the bad entry, keep converting the rest" from
    // "abort and return an empty bundle on any per-entry error".
    String bundle = """
      {"resourceType":"Bundle","type":"collection","entry":[
        {"resource":{"resourceType":"MedicationOrder","id":"13","status":"active",
         "dateWritten":"2023-10-20","patient":{"reference":"Patient/100"},
         "medicationCodeableConcept":{"coding":[{"system":"http://www.nlm.nih.gov/research/umls/rxnorm/","code":"1"}]}}},
        {"resource":{"resourceType":"NotAResource"}}
      ]}""";

    String r4 = ConverterService.convertBundleDstu2ToR4(bundle);

    assertTrue(r4.contains("MedicationRequest"), "the good entry should survive conversion");

    com.google.gson.JsonObject out = com.google.gson.JsonParser.parseString(r4).getAsJsonObject();
    com.google.gson.JsonArray outEntries = out.getAsJsonArray("entry");
    assertEquals(1, outEntries.size(),
        "exactly one entry should remain: the bad entry dropped, the good entry preserved");
  }
}
