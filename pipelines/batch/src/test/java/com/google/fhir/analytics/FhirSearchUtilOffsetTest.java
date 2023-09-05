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

import java.io.IOException;
import org.junit.Before;

public class FhirSearchUtilOffsetTest extends FhirSearchUtilTest {

  private static final String OFFSET_PARAM = "_offset=15";

  private static final String COUNT_PARAM = "_count=15";

  private static final String SORT_PARAM = "_sort=_id";

  private static final String SUMMARY_PARAM = "_summary=data";

  private static final String GIVEN_PARAM = "given=Bashir";

  @Before
  public void setup() throws IOException {
    super.setup("bundle_offset.json");
  }

  protected void assertBaseSearchUrl(String baseUrl) {
    assertThat(
        baseUrl,
        equalTo(
            BASE_URL
                + "?"
                + OFFSET_PARAM
                + "&"
                + COUNT_PARAM
                + "&"
                + SORT_PARAM
                + "&"
                + SUMMARY_PARAM
                + "&"
                + GIVEN_PARAM));
  }
}
