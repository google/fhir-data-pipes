// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.openmrs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
//import com.google.auth.http.HttpCredentialsAdapter;
//import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.BaseResource;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker<T extends BaseResource> implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

  private static final String FHIR_NAME = "projects/%s/locations/%s/datasets/%s/fhirStores/%s";
  private static final GsonFactory JSON_FACTORY = new GsonFactory();
  private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  // TODO: GoogleNetHttpTransport.newTrustedTransport()

  private String feedBaseUrl;
  private String jSessionId;
  private String resourceType;
  private Class<T> resourceClass;
  private FhirContext fhirContext = FhirContext.forR4();
  private String gcpProjectId;
  private String gcpLocation;
  private String gcpDatasetName;
  private String fhirStoreName;

  FhirEventWorker(String feedBaseUrl, String jSessionId, String resourceType,
      Class<T> resourceClass, String gcpProjectId, String gcpLocation,  String gcpDatasetName,
      String fhirStoreName) {
    this.feedBaseUrl = feedBaseUrl;
    this.jSessionId = jSessionId;
    this.resourceType = resourceType;
    this.resourceClass = resourceClass;
    this.gcpProjectId = gcpProjectId;
    this.gcpLocation = gcpLocation;
    this.gcpDatasetName = gcpDatasetName;
    this.fhirStoreName = fhirStoreName;
  }

  private String executeRequest(HttpUriRequest request) {
    try {
      // Execute the request and process the results.
      HttpClient httpClient = HttpClients.createDefault();
      HttpResponse response = httpClient.execute(request);
      HttpEntity responseEntity = response.getEntity();
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      responseEntity.writeTo(byteStream);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED
          && response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        log.error(String.format(
            "Exception for resource %s: %s", request.getURI().toString(),
            response.getStatusLine().toString()));
        log.error(byteStream.toString());
        throw new RuntimeException();
      }
      return byteStream.toString();
    } catch (IOException e) {
      log.error("Error in opening url: " + request.getURI().toString() + " exception: " + e);
      return "";
    }
  }

  private String fetchFhirResource(String urlStr) {
    try {
      URIBuilder uriBuilder = new URIBuilder(urlStr);
      HttpUriRequest request = RequestBuilder
          .get()
          .setUri(uriBuilder.build())
          .addHeader("Content-Type", "application/fhir+json")
          .addHeader("Accept-Charset", "utf-8")
          .addHeader("Accept", "application/fhir+json")
          .addHeader("Cookie", "JSESSIONID=" + this.jSessionId)
          .build();
      return executeRequest(request);
    } catch (URISyntaxException e) {
      log.error("Malformed FHIR url: " + urlStr + " exception: " + e);
      return "";
    }
  }

  private T parserFhirJson(String fhirJson) {
    IParser parser = fhirContext.newJsonParser();
    return parser.parseResource(resourceClass, fhirJson);
  }

  @Override
  public void process(Event event) {
    log.info("In process for event: " + event);
    String content = event.getContent();
    if (content == null || "".equals(content)) {
      log.warn("No content in event: " + event);
      return;
    }
    try {
      JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
      String fhirUrl = jsonObj.get("fhir").getAsString();
      if (fhirUrl == null || "".equals(fhirUrl)) {
        log.info("Skipping non-FHIR event " + event);
        return;
      }
      log.info("FHIR resource URL is: " + fhirUrl);
      String fhirJson = fetchFhirResource(feedBaseUrl + fhirUrl);
      log.info("Fetched FHIR resource: " + fhirJson);
      // Creating this resource is not really needed for the current purpose as we can simply
      // send the JSON payload to GCP FHIR store. This is kept for demonstration purposes.
      T resource = parserFhirJson(fhirJson);
      String resourceId = resource.getIdElement().getIdPart();
      log.info(String.format("Parsed FHIR resource ID is %s and IdBase is %s", resourceId,
          resource.getIdBase()));
      uploadToCloud(resourceId, fhirJson);
    } catch (JsonParseException e) {
      log.error(
          String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }

  // This follows the examples at:
  // https://github.com/GoogleCloudPlatform/java-docs-samples/healthcare/tree/master/healthcare/v1
  private void uploadToCloud(String resourceId, String jsonResource) {
    try {
      // TODO: Change these hardcoded values to configurable parameters.
      String fhirStoreName = String.format(
          FHIR_NAME, this.gcpProjectId, this.gcpLocation, this.gcpDatasetName, this.fhirStoreName);
      updateFhirResource(fhirStoreName, this.resourceType, resourceId, jsonResource);
    } catch (IOException e) {
      log.error(
          String.format("IOException while using Google APIS: %s", e.toString()));
    } catch (URISyntaxException e) {
      log.error(
          String.format("URI syntax exception while using Google APIS: %s", e.toString()));
    }
  }

  private void updateFhirResource(String fhirStoreName, String resourceType,
      String resourceId, String jsonResource)
      throws IOException, URISyntaxException {

    // Initialize the client, which will be used to interact with the service.
    CloudHealthcare client = createClient();
    String uri = String.format(
        "%sv1/%s/fhir/%s/%s", client.getRootUrl(), fhirStoreName, resourceType, resourceId);
    log.info(String.format("URL is: %s", uri));
    URIBuilder uriBuilder = new URIBuilder(uri);
    log.info(String.format("Full URL is: %s", uriBuilder.build()));
    StringEntity requestEntity = new StringEntity(jsonResource);

    HttpUriRequest request = RequestBuilder
        .put()
        .setUri(uriBuilder.build())
        .setEntity(requestEntity)
        .addHeader("Content-Type", "application/fhir+json")
        .addHeader("Accept-Charset", "utf-8")
        .addHeader("Accept", "application/fhir+json")
        .addHeader("Authorization", "Bearer " + getAccessToken())
        .build();
    log.info("Update FHIR resource response: " + executeRequest(request));
  }

  private CloudHealthcare createClient() throws IOException {
    final GoogleCredential credential = getGoogleCredential();
    HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {
      @Override
      public void initialize(HttpRequest httpRequest) throws IOException {
        credential.initialize(httpRequest);
        httpRequest.setConnectTimeout(60000); // 1 minute connect timeout
        httpRequest.setReadTimeout(60000); // 1 minute read timeout
      }
    };

    // Build the client for interacting with the service.
    return new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer)
        .setApplicationName("openmrs-fhir-warehouse")
        .build();
  }

  private String getAccessToken() throws IOException {
    GoogleCredential credential = getGoogleCredential();
    credential.refreshToken();
    return credential.getAccessToken();
  }

  private GoogleCredential getGoogleCredential() throws IOException {
    /*
    // TODO figure out why scope creation fails in this case.
    // Use Application Default Credentials (ADC) to authenticate the requests
    // For more information see https://cloud.google.com/docs/authentication/production
    final GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault()//;
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    credentials.refreshAccessToken();
    return credentials.getAccessToken().getTokenValue();
     */
    GoogleCredential credential =
        GoogleCredential.getApplicationDefault(HTTP_TRANSPORT, JSON_FACTORY)
            .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
    return credential;
  }

}
