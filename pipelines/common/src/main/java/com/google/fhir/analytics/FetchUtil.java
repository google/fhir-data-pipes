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
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import com.google.api.client.auth.oauth2.ClientCredentialsTokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.BasicAuthentication;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.time.Instant;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of utility functions for making FHIR API calls. */
public class FetchUtil {

  private static final Logger log = LoggerFactory.getLogger(FetchUtil.class);

  private final String fhirUrl;

  private final String sourceUser;

  private final String sourcePw;

  private final String oAuthTokenEndpoint;

  private final String oAuthClientId;

  private final String oAuthClientSecret;

  private final FhirContext fhirContext;

  private final IClientInterceptor authInterceptor;

  FetchUtil(
      String sourceFhirUrl,
      String sourceUser,
      String sourcePw,
      String oAuthTokenEndpoint,
      String oAuthClientId,
      String oAuthClientSecret,
      FhirContext fhirContext) {
    this.fhirUrl = sourceFhirUrl;
    this.sourceUser = Strings.nullToEmpty(sourceUser);
    this.sourcePw = Strings.nullToEmpty(sourcePw);
    this.oAuthTokenEndpoint = Strings.nullToEmpty(oAuthTokenEndpoint);
    this.oAuthClientId = Strings.nullToEmpty(oAuthClientId);
    this.oAuthClientSecret = Strings.nullToEmpty(oAuthClientSecret);
    this.fhirContext = fhirContext;
    Preconditions.checkState(
        this.oAuthTokenEndpoint.isEmpty()
            || (!this.oAuthClientId.isEmpty() && !this.oAuthClientSecret.isEmpty()));
    // Note we want to share the same `authInterceptor` between different FHIR clients to prevent
    // many unnecessary token fetch requests when OAuth is used.
    if (!this.oAuthTokenEndpoint.isEmpty()) {
      log.info("Fetching access tokens from {}", oAuthTokenEndpoint);
      authInterceptor =
          new ClientCredentialsAuthInterceptor(
              oAuthTokenEndpoint, oAuthClientId, oAuthClientSecret);
    } else if (!sourceUser.isEmpty()) {
      authInterceptor = new BasicAuthInterceptor(sourceUser, sourcePw);
    } else {
      authInterceptor = null;
    }
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

  public IGenericClient getSourceClient() {
    return getSourceClient(false);
  }

  public IGenericClient getSourceClient(boolean enableRequestLogging) {
    fhirContext.getRestfulClientFactory().setSocketTimeout(200 * 1000);

    IGenericClient client = fhirContext.getRestfulClientFactory().newGenericClient(fhirUrl);
    if (authInterceptor != null) {
      // TODO add logic to inspect/refresh token if we get 401/403 responses.
      client.registerInterceptor(authInterceptor);
    }

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

  /**
   * A simple class for fetching OAuth tokens through the client_credential grant flow and injecting
   * them to FHIR API calls. Instances of this class are thread-safe and should be shared between
   * threads for better performance and to reduce the load on the token endpoint.
   */
  private static class ClientCredentialsAuthInterceptor extends BearerTokenAuthInterceptor {

    private static final int TOKEN_REFRESH_LEEWAY_IN_SECONDS = 10;

    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private TokenResponse tokenResponse;
    private Instant nextRefresh;

    ClientCredentialsAuthInterceptor(String tokenEndpoint, String clientId, String clientSecret) {
      Preconditions.checkNotNull(tokenEndpoint);
      Preconditions.checkNotNull(clientId);
      Preconditions.checkNotNull(clientSecret);
      this.tokenEndpoint = tokenEndpoint;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
    }

    @Override
    public synchronized String getToken() {
      if (nextRefresh == null || Instant.now().isAfter(nextRefresh)) {
        try {
          log.debug("Fetching a new OAuth token; old refresh: {}", nextRefresh);
          tokenResponse = requestAccessToken();
          log.debug("Fetched token is: {}", tokenResponse.getAccessToken());
        } catch (IOException e) {
          String errorMessage = "Failed fetching a new token through client_credentials grant_type";
          log.error(errorMessage, e);
          throw new IllegalStateException(errorMessage);
        }
        nextRefresh =
            Instant.now()
                .plusSeconds(tokenResponse.getExpiresInSeconds() - TOKEN_REFRESH_LEEWAY_IN_SECONDS);
      }
      return tokenResponse.getAccessToken();
    }

    @Override
    public void interceptRequest(IHttpRequest theRequest) {
      theRequest.addHeader("Authorization", "Bearer " + getToken());
    }

    TokenResponse requestAccessToken() throws IOException {
      TokenResponse response =
          new ClientCredentialsTokenRequest(
                  new NetHttpTransport(), new GsonFactory(), new GenericUrl(tokenEndpoint))
              .setClientAuthentication(new BasicAuthentication(clientId, clientSecret))
              .execute();
      return response;
    }
  }
}
