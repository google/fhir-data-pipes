/*
 * Copyright 2020-2022 Google LLC
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
package org.openmrs.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirSearchUtilTest {

  private static final String PAGE_URL_PARAM = "_getpages=861af9b4-d847-4831-b945-ab1f6f08f03e";

  private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R4";

  private static final String SEARCH_URL = "Patient?given=TEST";

  @Mock private OpenmrsUtil openmrsUtil;

  @Mock private IGenericClient genericClient;

  @Mock private IUntypedQuery untypedQuery;

  @Mock private IQuery query;

  private Bundle bundle;

  private static FhirContext fhirContext;

  private FhirSearchUtil fhirSearchUtil;

  @BeforeClass
  public static void setupFhirContext() {
    fhirContext = FhirContext.forR4Cached();
  }

  @Before
  public void setup() throws IOException {
    URL url = Resources.getResource("bundle.json");
    String bundleStr = Resources.toString(url, StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    bundle = parser.parseResource(Bundle.class, bundleStr);
    fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
    when(openmrsUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
    when(openmrsUtil.getSourceClient()).thenReturn(genericClient);
    when(genericClient.search()).thenReturn(untypedQuery);
    when(untypedQuery.byUrl(SEARCH_URL)).thenReturn(query);
    when(query.count(anyInt())).thenReturn(query);
    when(query.summaryMode(any(SummaryEnum.class))).thenReturn(query);
    when(query.returnBundle(any())).thenReturn(query);
    when(query.execute()).thenReturn(bundle);
  }

  @Test
  public void testFindBaseSearchUrl() {
    String baseUrl = fhirSearchUtil.findBaseSearchUrl(bundle);
    assertThat(baseUrl, equalTo(BASE_URL + "?" + PAGE_URL_PARAM));
  }

  @Test
  public void testSearchForResource() {
    Bundle actualBundle = fhirSearchUtil.searchByUrl(SEARCH_URL, 10, SummaryEnum.DATA);
    assertThat(actualBundle.equalsDeep(bundle), equalTo(true));
  }

  @Test
  public void testGetNextUrlNull() {
    String bundleStr =
        "{ \"resourceType\": \"Bundle\",\n"
            + "    \"id\": \"3beab86b-ca1a-427b-8c5f-07635010c1d5\",\n"
            + "    \"meta\": { \"lastUpdated\": \"2021-04-22T05:08:33.750+03:00\" },\n"
            + "    \"type\": \"searchset\",\n"
            + "    \"total\": 0,\n"
            + "    \"link\": [\n"
            + "        {\n"
            + "            \"relation\": \"self\",\n"
            + "            \"url\":"
            + " \"http://domain.com/openmrs/ws/fhir2/R4/Encounter?_count=100&_summary=data\"\n"
            + "        }\n"
            + "    ]\n"
            + "}";
    IParser parser = fhirContext.newJsonParser();
    bundle = parser.parseResource(Bundle.class, bundleStr);
    String nextUrl = fhirSearchUtil.getNextUrl(bundle);
    assertThat(nextUrl, nullValue());
  }
}
