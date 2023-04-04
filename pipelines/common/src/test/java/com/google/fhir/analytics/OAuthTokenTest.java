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

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.when;

import com.google.api.client.auth.oauth2.TokenResponse;
import java.io.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OAuthTokenTest {

  OAuthToken.PasswordGrantCredentials pgCreds =
      new OAuthToken.PasswordGrantCredentials(
          "testClientId", "testClientSecret", "testUserName", "testPassword");

  String oidConnectUrl = "http://test_server/oid_url";

  @Mock OAuthToken.OauthTokenRequest oauthTokenRequest;

  TokenResponse testTokenResponse = new TokenResponse();

  @Before
  public void setUp() {
    testTokenResponse.setAccessToken("test_access_token");
    testTokenResponse.setExpiresInSeconds(24L);
  }

  @Test
  public void testBasic() throws Exception {

    when(oauthTokenRequest.generate()).thenReturn(testTokenResponse);

    assumeTrue(oidConnectUrl != null);
    assumeFalse(oidConnectUrl.isBlank());
    OAuthToken oaToken = new OAuthToken(oidConnectUrl, pgCreds);
    oaToken.tokenRequestGenerator = oauthTokenRequest;
    Assert.assertEquals(0L, oaToken.stats.numTokenRequests);

    Assert.assertNotNull(oaToken.getCurrentAccessToken());
    Assert.assertEquals(1L, oaToken.stats.numTokenRequests);

    Assert.assertNotNull(oaToken.getCurrentAccessToken());
    Assert.assertEquals(1L, oaToken.stats.numTokenRequests);

    oaToken.forceRefresh();
    Assert.assertNotNull(oaToken.getCurrentAccessToken());
    Assert.assertEquals(2L, oaToken.stats.numTokenRequests);
  }

  @Test
  public void testBadUrl() throws Exception {
    assumeTrue(oidConnectUrl != null);
    assumeFalse(oidConnectUrl.isBlank());
    OAuthToken oaToken = new OAuthToken(oidConnectUrl + "-xyz", pgCreds);
    when(oauthTokenRequest.generate()).thenThrow(IOException.class);
    oaToken.tokenRequestGenerator = oauthTokenRequest;

    try {
      oaToken.getCurrentAccessToken();
      Assert.fail("Exception should be thrown!");
    } catch (IOException ex) {
      // pass
    }

    Assert.assertEquals(0L, oaToken.stats.numTokenRequests, 0L);
    Assert.assertEquals(1L, oaToken.stats.numErrors, 2L);
  }
}
