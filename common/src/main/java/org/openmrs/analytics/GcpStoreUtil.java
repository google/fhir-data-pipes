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

package org.openmrs.analytics;

import ca.uhn.fhir.context.FhirContext;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
//import com.google.auth.http.HttpCredentialsAdapter;
//import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpStoreUtil extends FhirStoreUtil {
  private static final Logger log = LoggerFactory.getLogger(GcpStoreUtil.class);
  private static FhirContext fhirContext = FhirContext.forR4();

  private static final Pattern FHIR_PATTERN = Pattern.compile(
      "projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+");
  private static final GsonFactory JSON_FACTORY = new GsonFactory();
  private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();

  private String gcpFhirStore;

  GcpStoreUtil(String gcpFhirStore, String sourceFhirUrl, String sourceUser, String sourcePw) throws IllegalArgumentException {
    super(gcpFhirStore, sourceFhirUrl, sourceUser, sourcePw);

    Matcher fhirMatcher = FHIR_PATTERN.matcher(gcpFhirStore);
    if (!fhirMatcher.matches()) {
      throw new IllegalArgumentException(String.format(
          "The gcpFhirStore %s does not match %s pattern!", gcpFhirStore, FHIR_PATTERN));
    }
    this.gcpFhirStore = gcpFhirStore;
  }


  // This follows the examples at:
  // https://github.com/GoogleCloudPlatform/java-docs-samples/healthcare/tree/master/healthcare/v1
  @Override
  public void uploadResourceToCloud(String resourceId, Resource resource) {
    try {
      updateFhirResource(gcpFhirStore, resourceId, resource);
    } catch (IOException e) {
      log.error(
          String.format("IOException while using Google APIs: %s", e.toString()));
    } catch (URISyntaxException e) {
      log.error(
          String.format("URI syntax exception while using Google APIs: %s", e.toString()));
    }
  }

  private void updateFhirResource(String fhirStoreName, String resourceId, Resource resource) throws IOException, URISyntaxException {
    // Initialize the client, which will be used to interact with the service.
    CloudHealthcare client = createClient();
    String uri = String.format(
        "%sv1/%s/fhir/%s/%s", client.getRootUrl(), fhirStoreName, resource.getResourceType(), resourceId);
    URIBuilder uriBuilder = new URIBuilder(uri);
    log.info(String.format("Full URL is: %s", uriBuilder.build()));

    StringEntity requestEntity = new StringEntity(fhirContext.newJsonParser().encodeResourceToString(resource), StandardCharsets.UTF_8);

    HttpUriRequest request = RequestBuilder
        .put()
        .setUri(uriBuilder.build())
        .setEntity(requestEntity)
        .addHeader("Content-Type", "application/fhir+json")
        .addHeader("Accept-Charset", "utf-8")
        .addHeader("Accept", "application/fhir+json")
        .addHeader("Authorization", "Bearer " + getAccessToken())
        .build();
    String response = executeRequest(request);
    log.debug("Update FHIR resource response: " + response);
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

  @Override
  public FhirContext getFhirContext() {
    return fhirContext;
  }
}
