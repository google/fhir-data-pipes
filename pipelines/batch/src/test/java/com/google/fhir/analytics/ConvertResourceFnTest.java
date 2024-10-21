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
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.cerner.bunsen.exception.ProfileException;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConvertResourceFnTest {

  private ConvertResourceFn convertResourceFn;

  @Mock private ParquetUtil mockParquetUtil;

  @Captor private ArgumentCaptor<Resource> resourceCaptor;

  private void setUp(String args[]) throws PropertyVetoException, SQLException, ProfileException {
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    convertResourceFn =
        new ConvertResourceFn(options, "Test") {

          @Override
          public void setup() throws SQLException, ProfileException {
            super.setup();
            parquetUtil = mockParquetUtil;
          }
        };
    convertResourceFn.setup();
    AvroConversionUtil.initializeAvroConverters();
  }

  @Test
  public void testProcessPatientResource_withoutForcedId()
      throws IOException, java.text.ParseException, SQLException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    setUp(args);
    String patientResourceStr =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            null,
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "1",
            patientResourceStr);
    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("my-patient-id"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getResourceType().toString(), equalTo("Patient"));
    assertThat(((Patient) capturedResource).getGender().toString(), equalTo("MALE"));
  }

  @Test
  public void testProcessPatientResource_withForcedId()
      throws IOException, java.text.ParseException, SQLException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    setUp(args);
    String patientResourceStr =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            "forced-id-123",
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "1",
            patientResourceStr);
    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("forced-id-123"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getResourceType().toString(), equalTo("Patient"));
    assertThat(((Patient) capturedResource).getGender().toString(), equalTo("MALE"));
  }

  @Test
  public void testProcessDeletedPatientResourceFullMode()
      throws SQLException, IOException, ParseException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH", "--since="};
    setUp(args);
    // Deleted Patient resource
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            "forced-id-123",
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "2",
            "");
    convertResourceFn.writeResource(element);
    // Verify that the ParquetUtil writer is not invoked for the deleted resource.
    verify(mockParquetUtil, times(0)).write(Mockito.any());
  }

  @Test
  public void testProcessDeletedPatientResourceIncrementalMode()
      throws SQLException, IOException, ParseException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH", "--since=NON-EMPTY"};
    setUp(args);
    // Deleted Patient resource
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            "forced-id-123",
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "2",
            "");
    convertResourceFn.writeResource(element);

    // Verify the deleted resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("forced-id-123"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("2"));
    assertThat(
        capturedResource
            .getMeta()
            .getTag(ActionType.REMOVE.getSystem(), ActionType.REMOVE.toCode()),
        notNullValue());
    assertThat(capturedResource.getResourceType().toString(), equalTo("Patient"));
    assertThat(
        capturedResource.getClass().getName(),
        equalTo(org.hl7.fhir.r4.model.Patient.class.getName()));
  }

  @Test
  public void testResourceMetaTags()
      throws IOException, java.text.ParseException, SQLException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH", "--since="};
    setUp(args);
    String patientResourceStr =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            "forced-id-123",
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "1",
            patientResourceStr);
    // Set Tag of HAPI FHIR tag type 0
    Coding coding0 = new Coding("system0", "code0", "display0");
    ResourceTag tag0 = new ResourceTag(coding0, "123", 0);
    // Set Tag of HAPI FHIR tag type 1
    Coding coding1 = new Coding("system1", "code1", "display1");
    ResourceTag tag1 = new ResourceTag(coding1, "123", 1);
    // Set Tag of HAPI FHIR tag type 2
    Coding coding2 = new Coding("system2", "code2", "display2");
    ResourceTag tag2 = new ResourceTag(coding2, "123", 2);
    element.setTags(List.of(tag0, tag1, tag2));
    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("forced-id-123"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getMeta().getTag().get(0).getSystem(), equalTo("system0"));
    assertThat(capturedResource.getMeta().getTag().get(0).getCode(), equalTo("code0"));
    assertThat(capturedResource.getMeta().getTag().get(0).getDisplay(), equalTo("display0"));
    assertThat(capturedResource.getMeta().getProfile().get(0).asStringValue(), equalTo("code1"));
    assertThat(capturedResource.getMeta().getSecurity().get(0).getCode(), equalTo("code2"));
    assertThat(capturedResource.getMeta().getSecurity().get(0).getSystem(), equalTo("system2"));
    assertThat(capturedResource.getMeta().getSecurity().get(0).getDisplay(), equalTo("display2"));
  }

  @Test
  public void testProcessEncounterResource_withMdmLinks()
      throws IOException, java.text.ParseException, SQLException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    setUp(args);
    String encounterResourceStr =
        Resources.toString(Resources.getResource("encounter.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "456",
            "mock-id",
            null,
            "Encounter",
            "2020-09-19 12:09:23",
            "R4",
            "1",
            encounterResourceStr);

    // Add MdmLinks to the HapiRowDescriptor
    MdmLink mdmLink1 =
        MdmLink.builder()
            .resourceId("456")
            .sourceFhirId("f71e5b02-4a5a-44f8-82ec-c5a215ad886c")
            .goldenFhirId("golden-id-1")
            .build();
    element.setMdmLinks(List.of(mdmLink1));

    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("mock-id"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getResourceType().toString(), equalTo("Encounter"));
    assertThat(
        ((Encounter) capturedResource).getSubject().getReference(), equalTo("Patient/golden-id-1"));
  }

  @Test
  public void testProcessPatientResource_withSourceIdentifiers()
      throws IOException, java.text.ParseException, SQLException, PropertyVetoException,
          ViewApplicationException, ProfileException {
    String[] args = {"--outputParquetPath=SOME_PATH"};
    setUp(args);
    String patientResourceStr =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);
    HapiRowDescriptor element =
        HapiRowDescriptor.create(
            "123",
            "my-patient-id",
            "forced-id-123",
            "Patient",
            "2020-09-19 12:09:23",
            "R4",
            "1",
            patientResourceStr);

    SourceIdentifier sourceIdentifier1 =
        SourceIdentifier.builder().resourceId("123").system("system-1").value("value-1").build();

    SourceIdentifier sourceIdentifier2 =
        SourceIdentifier.builder().resourceId("123").system("system-2").value("value-2").build();

    element.setSourceIdentifiers(List.of(sourceIdentifier1, sourceIdentifier2));

    convertResourceFn.writeResource(element);

    // Verify the resource is sent to the writer.
    verify(mockParquetUtil).write(resourceCaptor.capture());
    Resource capturedResource = resourceCaptor.getValue();
    assertThat(capturedResource.getId(), equalTo("forced-id-123"));
    assertThat(capturedResource.getMeta().getVersionId(), equalTo("1"));
    assertThat(
        capturedResource.getMeta().getLastUpdated(),
        equalTo(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2020-09-19 12:09:23")));
    assertThat(capturedResource.getResourceType().toString(), equalTo("Patient"));

    List<Identifier> identifiers = ((Patient) capturedResource).getIdentifier();
    assertThat(identifiers, notNullValue());
    assertThat(identifiers.size(), equalTo(2));
    assertThat(identifiers.get(0).getSystem(), equalTo("system-1"));
    assertThat(identifiers.get(0).getValue(), equalTo("value-1"));
    assertThat(identifiers.get(1).getSystem(), equalTo("system-2"));
    assertThat(identifiers.get(1).getValue(), equalTo("value-2"));
  }
}
