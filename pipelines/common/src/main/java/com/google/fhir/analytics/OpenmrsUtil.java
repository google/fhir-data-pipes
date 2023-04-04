/*
 * Copyright 2020-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import java.io.IOException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenmrsUtil {

  private static final Logger log = LoggerFactory.getLogger(OpenmrsUtil.class);

  private final String fhirUrl;

  private final String sourceUser;

  private final String sourcePw;

  private final String oidConnectUrl;

  private final String clientId;

  private final String clientSecret;

  private final String oAuthUsername;

  private final String oAuthPassword;

  private final FhirContext fhirContext;

  OpenmrsUtil(
      String sourceFhirUrl,
      String sourceUser,
      String sourcePw,
      String oidConnectUrl,
      String clientId,
      String clientSecret,
      String oAuthUsername,
      String oAuthPassword,
      FhirContext fhirContext)
      throws IllegalArgumentException {
    this.fhirUrl = sourceFhirUrl;
    this.sourceUser = sourceUser;
    this.sourcePw = sourcePw;
    this.oidConnectUrl = oidConnectUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.oAuthUsername = oAuthUsername;
    this.oAuthPassword = oAuthPassword;
    this.fhirContext = fhirContext;
  }

  public Resource fetchFhirResource(String resourceType, String resourceId) {
    try {
      // Create client
      IGenericClient client = getSourceClient();
      // TODO add summary mode for data only.
      IBaseResource resource = client.read().resource(resourceType).withId(resourceId).execute();
      return (Resource) resource;
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed fetching FHIR %s resource with Id %s: %s", resourceType, resourceId, e));
      return null;
    }
  }

  public Resource fetchFhirResource(String resourceUrl) {
    // Parse resourceUrl
    String[] sepUrl = resourceUrl.split("/");
    String resourceId = sepUrl[sepUrl.length - 1];
    String resourceType = sepUrl[sepUrl.length - 2];
    return fetchFhirResource(resourceType, resourceId);
  }

  public IGenericClient getSourceClient() throws IOException {
    return getSourceClient(false);
  }

  public IGenericClient getSourceClient(boolean enableRequestLogging) throws IOException {
    IGenericClient client = fhirContext.getRestfulClientFactory().newGenericClient(this.fhirUrl);
    if (this.oidConnectUrl != null && !oidConnectUrl.isBlank()) {
      OAuthToken oaToken =
          new OAuthToken(
              this.oidConnectUrl,
              new OAuthToken.PasswordGrantCredentials(
                  this.clientId, this.clientSecret, this.oAuthUsername, this.oAuthPassword));

      IClientInterceptor authInterceptor =
          new BearerTokenAuthInterceptor(oaToken.getCurrentAccessToken());
      client.registerInterceptor(authInterceptor);
    } else {
      IClientInterceptor authInterceptor = new BasicAuthInterceptor(this.sourceUser, this.sourcePw);
      client.registerInterceptor(authInterceptor);
    }

    fhirContext.getRestfulClientFactory().setSocketTimeout(200 * 1000);

    if (enableRequestLogging) {
      LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
      loggingInterceptor.setLogger(log);
      loggingInterceptor.setLogRequestSummary(true);
      client.registerInterceptor(loggingInterceptor);
    }

    return client;
  }

  public String getSourceFhirUrl() {
    return fhirUrl;
  }
}
