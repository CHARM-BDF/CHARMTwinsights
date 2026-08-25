package org.charm.converter;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test against the real {@link ConverterServer} HTTP handlers
 * (not a reimplementation): starts the actual server on an ephemeral port
 * and drives it with real HTTP requests via {@link HttpClient}.
 */
class ConverterServerTest {

  private HttpServer server;
  private HttpClient client;
  private String baseUrl;

  @BeforeEach
  void startServer() throws Exception {
    server = ConverterServer.start(0); // ephemeral port
    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
    client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  }

  @AfterEach
  void stopServer() {
    if (server != null) server.stop(0);
  }

  @Test
  void convertReturns200WithConvertedResource() throws Exception {
    String requestBody = """
      {"sourceVersion":"DSTU2","bundle":
        {"resourceType":"Bundle","type":"collection","entry":[
          {"resource":{"resourceType":"MedicationOrder","id":"13","status":"active",
           "dateWritten":"2023-10-20","patient":{"reference":"Patient/100"},
           "medicationCodeableConcept":{"coding":[{"system":"http://www.nlm.nih.gov/research/umls/rxnorm/","code":"1"}]}}}
        ]}}""";

    HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/convert"))
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(200, response.statusCode());
    assertTrue(response.body().contains("MedicationRequest"));
  }

  @Test
  void convertMissingBundleReturns400WithOperationOutcome() throws Exception {
    String requestBody = "{\"sourceVersion\":\"DSTU2\"}";

    HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/convert"))
        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(400, response.statusCode());
    assertTrue(response.body().contains("\"resourceType\":\"OperationOutcome\""));
  }

  @Test
  void convertWrongMethodReturns405() throws Exception {
    HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/convert"))
        .GET()
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(405, response.statusCode());
  }
}
