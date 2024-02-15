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

// TODO add testes for DSTU3 resources too.

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.hamcrest.Matchers;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroConversionUtilTest {
  private AvroConversionUtil instance;
  private FhirContext fhirContext;
  private String patientBundle;
  private String observationBundle;

  public static final String US_CORE_PATIENT =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  @Before
  public void setup()
      throws IOException, ClassNotFoundException, URISyntaxException, ProfileMapperException {
    AvroConversionUtil.initializeAvroConverters();
    patientBundle =
        Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
    observationBundle =
        Resources.toString(
            Resources.getResource("observation_bundle.json"), StandardCharsets.UTF_8);

    URL resourceURL =
        AvroConversionUtilTest.class
            .getClassLoader()
            .getResource("definitions-r4/StructureDefinition-us-core-patient.json");
    File file = Paths.get(resourceURL.toURI()).toFile();

    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.R4);
    AvroConversionUtil.getInstance().deRegisterMappingsFor(FhirVersionEnum.R4);

    this.fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFor(FhirVersionEnum.R4, file.getParentFile().getAbsolutePath());
    this.instance =
        AvroConversionUtil.getInstance()
            .loadContextFor(FhirVersionEnum.R4, file.getParentFile().getAbsolutePath());
  }

  @Test
  public void getResourceSchema_Patient() throws ProfileMapperException {
    Schema schema = instance.getResourceSchema("Patient", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("name").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Observation() throws ProfileMapperException {
    Schema schema = instance.getResourceSchema("Observation", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("basedOn").toString(), notNullValue());
    assertThat(schema.getField("subject").toString(), notNullValue());
    assertThat(schema.getField("code").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Encounter() throws ProfileMapperException {
    Schema schema = instance.getResourceSchema("Encounter", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("status").toString(), notNullValue());
    assertThat(schema.getField("class").toString(), notNullValue());
  }

  @Test
  public void generateRecords_BundleOfPatients() throws ProfileMapperException {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    List<GenericRecord> recordList = instance.generateRecords(bundle, fhirContext);
    assertThat(recordList.size(), equalTo(1));
    assertThat(recordList.get(0).get("address"), notNullValue());
    Collection<Object> addressList = (Collection<Object>) recordList.get(0).get("address");
    assertThat(addressList.size(), equalTo(1));
    Record address = (Record) addressList.iterator().next();
    assertThat((String) address.get("city"), equalTo("Waterloo"));
  }

  @Test
  public void generateRecords_BundleOfObservations() throws ProfileMapperException {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    List<GenericRecord> recordList = instance.generateRecords(bundle, fhirContext);
    assertThat(recordList.size(), equalTo(6));
  }

  @Test
  public void generateRecordForPatient() throws ProfileMapperException {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    GenericRecord record =
        instance.convertToAvro(bundle.getEntry().get(0).getResource(), fhirContext);
    Collection<Object> addressList = (Collection<Object>) record.get("address");
    // TODO We need to fix this again; the root cause is the change in `id` type to System.String.
    // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55
    // assertThat(record.get("id"), equalTo("471be3bc-08c7-4d78-a4ab-1b3d044dae67"));
    assertThat(addressList.size(), equalTo(1));
    Record address = (Record) addressList.iterator().next();
    assertThat((String) address.get("city"), equalTo("Waterloo"));
  }

  @Test
  public void convertObservationWithBigDecimalValue() throws IOException, ProfileMapperException {
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    GenericRecord record = instance.convertToAvro(observation, fhirContext);
    GenericData.Record valueRecord = (GenericData.Record) record.get("value");
    Double value = (Double) ((GenericData.Record) valueRecord.get("quantity")).get("value");
    assertThat(value, closeTo(25, 0.001));
  }

  @Test
  public void checkForCorrectAvroConverter() throws ProfileMapperException {
    AvroConverter patientUtilAvroConverter = instance.getConverter("Patient", fhirContext);
    AvroConverter patientDirectAvroConverter =
        AvroConverter.forResource(fhirContext, US_CORE_PATIENT);
    // Check if the schemas are equal
    assertThat(
        patientUtilAvroConverter.getSchema(),
        Matchers.equalTo(patientDirectAvroConverter.getSchema()));

    AvroConverter obsUtilAvroConverter = instance.getConverter("Observation", fhirContext);
    AvroConverter obsDirectAvroConverter = AvroConverter.forResource(fhirContext, "Observation");
    // Check if the schemas are equal
    assertThat(
        obsUtilAvroConverter.getSchema(), Matchers.equalTo(obsDirectAvroConverter.getSchema()));
  }

  @Test
  public void testForAvroRecords() throws IOException, ProfileMapperException {
    IBaseResource baseResource = loadResource("patient_us_core.json", Patient.class);
    GenericRecord avroRecord = instance.convertToAvro((Resource) baseResource, fhirContext);

    AvroConverter avroConverter = instance.getConverter("Patient", fhirContext);
    IBaseResource baseResource1 = avroConverter.avroToResource(avroRecord);

    IParser jsonParser = fhirContext.newJsonParser();
    String string1 = (jsonParser.encodeResourceToString(baseResource));
    String string2 = jsonParser.encodeResourceToString(baseResource1);

    assertThat(string1.equals(string2), Matchers.equalTo(Boolean.TRUE));
  }

  private <T extends IBaseResource> IBaseResource loadResource(
      String resourceFile, Class<T> resourceType) throws IOException {
    IParser jsonParser = FhirContext.forR4().newJsonParser();
    try (InputStream patientStream =
        getClass().getClassLoader().getResourceAsStream(resourceFile)) {
      return jsonParser.parseResource(resourceType, patientStream);
    }
  }
}
