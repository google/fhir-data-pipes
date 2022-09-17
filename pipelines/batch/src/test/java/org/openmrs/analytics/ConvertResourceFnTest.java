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
import static org.mockito.Mockito.verify;

import com.google.common.io.Resources;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConvertResourceFnTest {

  private ConvertResourceFn convertResourceFn;

  @Mock private ParquetUtil mockParquetUtil;

  @Captor private ArgumentCaptor<Resource> resourceCaptor;

  @Before
  public void setUp() throws PropertyVetoException, SQLException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    convertResourceFn =
        new ConvertResourceFn(options, "Test") {

          @Override
          public void setup() throws PropertyVetoException, SQLException {
            super.setup();
            parquetUtil = mockParquetUtil;
          }
        };
    convertResourceFn.setup();
    ParquetUtil.initializeAvroConverters();
  }

  @Test
  public void testProcessPatientResource()
      throws IOException, java.text.ParseException, SQLException {
    String patientResourceStr =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create("123", "Patient", "2020-09-19 12:09:23", "1", patientResourceStr);
    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("123"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getResourceType().toString(), equalTo("Patient"));
    assertThat(((Patient) capturedResource).getGender().toString(), equalTo("MALE"));
  }
}
