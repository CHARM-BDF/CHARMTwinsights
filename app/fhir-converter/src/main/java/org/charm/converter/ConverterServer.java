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
