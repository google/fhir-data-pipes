/*
 * Copyright 2020-2025 Google LLC
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.impl.RestfulClientFactory;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("NullAway.Init")
@ExtendWith(MockitoExtension.class)
public class FetchUtilTest {

  private static final String SOURCE_FHIR_URL = "someurl";

  private static final String RESOURCE_ID = "someid";

  @Mock FhirContext fhirContext;

  @Mock RestfulClientFactory clientFactory;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  IGenericClient client;

  FetchUtil fetchUtil;

  // Suppressing because fhirContext.getRestfulClientFactory() is a false positive for
  // DirectInvocationOnMock
  @SuppressWarnings("DirectInvocationOnMock")
  @BeforeEach
  public void setUp() throws Exception {
    fetchUtil =
        new FetchUtil(
            SOURCE_FHIR_URL,
            "someuser",
            "somepw",
            "someOAuthEndpoint",
            "someOAuthClient",
            "someOAuthSecret",
            true,
            fhirContext);

    doNothing().when(clientFactory).setSocketTimeout(any(Integer.class));
    when(fhirContext.getRestfulClientFactory()).thenReturn(clientFactory);
    when(fhirContext.getRestfulClientFactory().newGenericClient(SOURCE_FHIR_URL))
        .thenReturn(client);
    doNothing().when(client).registerInterceptor(any(IClientInterceptor.class));
  }

  @Test
  public void shouldFetchFhirResource() {
    Patient testResource = new Patient();
    String resourceType = testResource.getResourceType().name();

    testResource.setId(RESOURCE_ID);
    String resourceUrl = "fhirendpoint/" + resourceType + "/" + RESOURCE_ID;

    when(client.read().resource(resourceType).withId(RESOURCE_ID).execute())
        .thenReturn(testResource);

    Patient result = (Patient) fetchUtil.fetchFhirResource(resourceUrl);

    assertThat(result, equalTo(testResource));
  }

  @Test
  public void shouldGetSourceClient() {
    IGenericClient result = fetchUtil.getSourceClient();

    assertThat(result, equalTo(client));
  }
}
