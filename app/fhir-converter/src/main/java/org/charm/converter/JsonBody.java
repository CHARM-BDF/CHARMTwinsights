package org.charm.converter;

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
