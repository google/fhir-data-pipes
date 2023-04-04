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

import com.google.api.client.auth.oauth2.*;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class OAuthToken implements Serializable {

  static final JsonFactory JSON_FACTORY = new GsonFactory();
  static final NetHttpTransport TRANSPORT_FACTORY = new NetHttpTransport();

  public static class PasswordGrantCredentials implements Serializable {

    public final String clientId;
    public final String clientSecret;
    public final String username;
    public final String password;

    public PasswordGrantCredentials(
        String clientId, String clientSecret, String username, String password) {
      this.clientId = clientId;
      this.clientSecret = clientSecret;
      this.username = username;
      this.password = password;
    }
  }

  protected NetHttpTransport transportFactory;
  protected String oidConnectUrl;
  protected PasswordGrantCredentials pgCreds;

  protected transient HttpTransport transport = null;
  protected transient TokenResponse tokenResponse = null;
  protected transient Instant refreshAt = null;

  public static class Stats {
    long numTokenRequests = 0;
    long numErrors = 0;
  }

  public OauthTokenRequest tokenRequestGenerator = new OauthTokenRequest();
  public transient Stats stats = null;

  public OAuthToken(String oidConnectUrl, PasswordGrantCredentials pgCreds) {
    this(TRANSPORT_FACTORY, oidConnectUrl, pgCreds);
  }

  /**
   * @param oidConnectUrl = "https://<authority>/.../openid-connect";
   */
  public OAuthToken(
      NetHttpTransport transportFactory, String oidConnectUrl, PasswordGrantCredentials pgCreds) {

    this.transportFactory = transportFactory;
    this.oidConnectUrl = oidConnectUrl;
    this.pgCreds = pgCreds;

    transientInit();
  }

  protected void transientInit() {
    this.transport = transportFactory;
    this.stats = new Stats();
  }

  private void readObject(ObjectInputStream input) throws ClassNotFoundException, IOException {
    input.defaultReadObject();
    transientInit();
  }

  public class OauthTokenRequest implements Serializable {

    public TokenResponse generate() throws IOException {
      String tokenUrl = oidConnectUrl + "/token";
      PasswordTokenRequest tokenRequest =
          new PasswordTokenRequest(
                  transport,
                  JSON_FACTORY,
                  new GenericUrl(tokenUrl),
                  pgCreds.username,
                  pgCreds.password)
              .setClientAuthentication(
                  new ClientParametersAuthentication(pgCreds.clientId, pgCreds.clientSecret));
      return tokenRequest.execute();
    }
  }

  protected void maybeRefreshToken() throws IOException {
    if (tokenResponse != null && refreshAt != null && Instant.now().isBefore(refreshAt)) return;

    try {
      tokenResponse = tokenRequestGenerator.generate();
      refreshAt = Instant.now().plus(Duration.ofSeconds(tokenResponse.getExpiresInSeconds() / 2));
      ++stats.numTokenRequests;
    } catch (IOException ex) {
      tokenResponse = null;
      refreshAt = null;
      ++stats.numErrors;
      throw ex;
    }
  }

  public String getCurrentAccessToken() throws IOException {
    maybeRefreshToken();
    return tokenResponse.getAccessToken();
  }

  public void forceRefresh() {
    this.refreshAt = null;
  }
}
