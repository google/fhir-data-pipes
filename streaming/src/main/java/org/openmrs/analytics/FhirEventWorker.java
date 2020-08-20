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
import ca.uhn.fhir.parser.IParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.BaseResource;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker<T extends BaseResource> implements EventWorker {
  private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

  private String feedBaseUrl;
  private String jSessionId;
  private String resourceType;
  private Class<T> resourceClass;
  private FhirContext fhirContext = FhirContext.forR4();
  private FhirStoreUtil fhirStoreUtil;

  FhirEventWorker(String feedBaseUrl, String jSessionId, String resourceType,
      Class<T> resourceClass, String gcpFhirStore) {
    this.feedBaseUrl = feedBaseUrl;
    this.jSessionId = jSessionId;
    this.resourceType = resourceType;
    this.resourceClass = resourceClass;
    // TODO inject dependency.
    this.fhirStoreUtil = new FhirStoreUtil(gcpFhirStore);
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
          // TODO(bashir2): Switch to BasicAuth instead of relying on cookies.
          .addHeader("Cookie", "JSESSIONID=" + this.jSessionId)
          .build();
      return fhirStoreUtil.executeRequest(request);
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
      fhirStoreUtil.uploadResourceToCloud(this.resourceType, resourceId, fhirJson);
    } catch (JsonParseException e) {
      log.error(
          String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
    }
  }

  @Override
  public void cleanUp(Event event) {
    log.info("In clean-up!");
  }

}
