package org.charm.converter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hl7.fhir.convertors.factory.VersionConvertorFactory_10_40;
import org.hl7.fhir.dstu2.model.Resource;

import java.nio.charset.StandardCharsets;

public final class ConverterService {
  private ConverterService() {}

  /**
   * Convert a DSTU2 bundle JSON to an R4 collection bundle JSON, entry by entry.
   *
   * <p>Deliberately does NOT parse the DSTU2 bundle as a whole with HAPI's DSTU2
   * {@code JsonParser}: that parser eagerly parses every entry's resource while
   * walking the bundle, so a single unrecognized/unconvertible entry resource
   * would abort parsing of the entire bundle. Instead the raw JSON structure is
   * walked with Gson, and each entry's resource JSON is parsed and converted
   * independently so a bad entry can be skipped without losing the rest.
   */
  public static String convertBundleDstu2ToR4(String dstu2BundleJson) {
    try {
      JsonElement rootEl = com.google.gson.JsonParser.parseString(dstu2BundleJson);
      if (!rootEl.isJsonObject()) {
        throw new IllegalArgumentException("Input is not a JSON object");
      }
      JsonObject root = rootEl.getAsJsonObject();
      String resourceType = root.has("resourceType") ? root.get("resourceType").getAsString() : null;
      if (!"Bundle".equals(resourceType)) {
        throw new IllegalArgumentException("Top-level resource is not a Bundle");
      }

      org.hl7.fhir.r4.model.Bundle out = new org.hl7.fhir.r4.model.Bundle();
      out.setType(org.hl7.fhir.r4.model.Bundle.BundleType.COLLECTION);

      JsonArray entries = root.has("entry") ? root.getAsJsonArray("entry") : new JsonArray();
      org.hl7.fhir.dstu2.formats.JsonParser d2 = new org.hl7.fhir.dstu2.formats.JsonParser();

      for (JsonElement entryEl : entries) {
        if (!entryEl.isJsonObject()) continue;
        JsonObject entryObj = entryEl.getAsJsonObject();
        if (!entryObj.has("resource") || !entryObj.get("resource").isJsonObject()) continue;
        String resourceJson = entryObj.get("resource").toString();
        try {
          Resource dstu2Resource =
              d2.parse(resourceJson.getBytes(StandardCharsets.UTF_8));
          org.hl7.fhir.r4.model.Resource r4res =
              (org.hl7.fhir.r4.model.Resource) VersionConvertorFactory_10_40.convertResource(dstu2Resource);
          out.addEntry().setResource(r4res);
        } catch (Exception skip) {
          // Per-resource failure (unparseable or unconvertible): skip this entry, keep converting the rest.
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
