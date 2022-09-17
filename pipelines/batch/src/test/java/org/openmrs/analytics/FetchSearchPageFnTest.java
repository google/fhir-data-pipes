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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FetchSearchPageFnTest {

  private FetchSearchPageFn<String> fetchSearchPageFn;

  private FhirContext fhirContext;

  @Mock private ParquetUtil mockParquetUtil;

  @Captor private ArgumentCaptor<Bundle> bundleCaptor;

  @Before
  public void setUp() throws SQLException, PropertyVetoException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    fetchSearchPageFn =
        new FetchSearchPageFn<String>(options, "TEST") {

          @Override
          public void setup() throws SQLException, PropertyVetoException {
            super.setup();
            parquetUtil = mockParquetUtil;
          }
        };
    this.fhirContext = FhirContext.forR4();
    fetchSearchPageFn.setup();
    ParquetUtil.initializeAvroConverters();
  }

  @Test
  public void testProcessObservationBundle() throws IOException, SQLException {
    String observationBundleStr =
        Resources.toString(
            Resources.getResource("observation_decimal_bundle.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundleStr);
    fetchSearchPageFn.processBundle(bundle);

    // Verify the bundle is sent to the writer.
    verify(mockParquetUtil).writeRecords(bundleCaptor.capture(), isNull());
    Bundle capturedBundle = bundleCaptor.getValue();
    assertThat(bundle, equalTo(capturedBundle));
  }
}
