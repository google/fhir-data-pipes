/*
 * Copyright 2020-2024 Google LLC
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
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReadNDJsonFilesTest {

  private ReadNDJsonFilesFn readNDJsonFilesFn;

  private Bundle capturedBundle;

  @Mock private FileIO.ReadableFile fileMock;

  @Before
  public void setUp() throws PropertyVetoException, SQLException, ProfileMapperException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    readNDJsonFilesFn =
        new ReadNDJsonFilesFn(options) {
          @Override
          protected void processBundle(Bundle bundle, @Nullable Set<String> resourceTypes) {
            capturedBundle = bundle;
          }
        };
    readNDJsonFilesFn.setup();
  }

  @Test
  public void testReadingNDJsonFile()
      throws IOException, SQLException, ViewApplicationException, ProfileMapperException {

    ResourceId resourceId =
        FileSystems.matchNewResource(Resources.getResource("patients.ndjson").getFile(), false);
    ReadableByteChannel readableByteChannel = org.apache.beam.sdk.io.FileSystems.open(resourceId);
    when(fileMock.open()).thenReturn(readableByteChannel);

    readNDJsonFilesFn.processElement(fileMock);

    // Verify the parsed resource.
    assertThat(capturedBundle, notNullValue());
    assertThat(capturedBundle.getEntry().size(), equalTo(3));
    Patient patient = (Patient) capturedBundle.getEntry().get(0).getResource();
    assertThat(patient.getIdElement().getIdPart(), equalTo("5c41cecf-cf81-434f-9da7-e24e5a99dbc2"));
  }
}
