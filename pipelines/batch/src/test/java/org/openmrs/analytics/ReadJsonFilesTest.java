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
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

import com.google.common.io.Resources;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReadJsonFilesTest {

  private ReadJsonFilesFn readJsonFilesFn;

  @Mock private FileIO.ReadableFile fileMock;

  private Bundle capturedBundle;

  @Before
  public void setUp() throws PropertyVetoException, SQLException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    readJsonFilesFn =
        new ReadJsonFilesFn(options) {

          @Override
          protected void processBundle(Bundle bundle, @Nullable Set<String> resourceTypes) {
            capturedBundle = bundle;
          }
        };
    readJsonFilesFn.setup();
  }

  @Test
  public void testProcessBundleUrnRef() throws IOException, java.text.ParseException, SQLException {
    String bundleResourceStr =
        Resources.toString(Resources.getResource("bundle_urn_ref.json"), StandardCharsets.UTF_8);
    when(fileMock.readFullyAsUTF8String()).thenReturn(bundleResourceStr);
    readJsonFilesFn.processElement(fileMock, null);

    // Verify the parsed resource.
    assertThat(capturedBundle, notNullValue());
    assertThat(capturedBundle.getEntry().size(), equalTo(2));
    Observation obs = (Observation) capturedBundle.getEntry().get(1).getResource();
    assertThat(obs.getIdElement().getIdPart(), equalTo("07ee194d-ce49-4cab-3a62-5b1a60b59726"));
    // The referenced Patient is present in the Bundle, hence the reference should be updated.
    assertThat(
        obs.getSubject().getReference(), equalTo("Patient/437a1b3e-1b17-ae3c-24f8-6079abe1ddc4"));
    // The referenced Encounter is not present in the Bundle.
    assertThat(
        obs.getEncounter().getReference(),
        equalTo("urn:uuid:2b442f5f-4f54-75ae-977b-1388db732966"));
  }

  @Test
  public void testProcessBundleRelativeRef()
      throws IOException, java.text.ParseException, SQLException {
    String bundleResourceStr =
        Resources.toString(
            Resources.getResource("bundle_relative_ref.json"), StandardCharsets.UTF_8);
    when(fileMock.readFullyAsUTF8String()).thenReturn(bundleResourceStr);
    readJsonFilesFn.processElement(fileMock, null);

    // Verify the parsed resource.
    assertThat(capturedBundle, notNullValue());
    assertThat(capturedBundle.getEntry().size(), equalTo(2));
    Observation obs = (Observation) capturedBundle.getEntry().get(0).getResource();
    assertThat(obs.getIdElement().getIdPart(), equalTo("751002"));
    assertThat(obs.getSubject().getReference(), equalTo("Patient/749605"));
    assertThat(obs.getEncounter().getReference(), equalTo("Encounter/750983"));
  }
}
