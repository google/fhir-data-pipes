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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirSearchUtilGetPagesTest extends FhirSearchUtilTest {

  private static final String PAGE_URL_PARAM = "_getpages=861af9b4-d847-4831-b945-ab1f6f08f03e";

  private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R4";

  private static final String SEARCH_URL = "Patient?given=TEST";

  @Before
  public void setup() throws IOException {
    when(openmrsUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
    super.setup("bundle_getpages.json");
  }

  protected void assertBaseSearchUrl(String baseUrl) {
    assertThat(baseUrl, equalTo(BASE_URL + "?" + PAGE_URL_PARAM));
  }
}
