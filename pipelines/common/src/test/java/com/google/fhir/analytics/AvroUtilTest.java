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
import static org.hamcrest.number.IsCloseTo.closeTo;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
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

public class AvroUtilTest {

  public static final String US_CORE_PATIENT =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";
  private FhirContext fhirContext;

  @Before
  public void init() throws URISyntaxException, ClassNotFoundException {
    this.getClass().getClassLoader().loadClass(FhirContexts.class.getName());
    URL resourceURL =
        AvroUtilTest.class
            .getClassLoader()
            .getResource("definitions-r4/StructureDefinition-us-core-patient.json");
    File file = Paths.get(resourceURL.toURI()).toFile();
    System.out.println("Test INIT");
    FhirContexts.deRegisterFhirContext(FhirVersionEnum.R4);
    AvroUtil.deRegisterConverters(FhirVersionEnum.R4);
    this.fhirContext = FhirContexts.forR4(Arrays.asList(file.getParentFile().getAbsolutePath()));
  }

  @Test
  public void checkForCorrectAvroConverter() throws URISyntaxException {
    AvroConverter patientUtilAvroConverter = AvroUtil.getConverter("Patient", fhirContext);
    AvroConverter patientDirectAvroConverter =
        AvroConverter.forResource(fhirContext, US_CORE_PATIENT);
    // Check if the schemas are equal
    assertThat(
        patientUtilAvroConverter.getSchema(),
        Matchers.equalTo(patientDirectAvroConverter.getSchema()));

    AvroConverter obsUtilAvroConverter = AvroUtil.getConverter("Observation", fhirContext);
    AvroConverter obsDirectAvroConverter = AvroConverter.forResource(fhirContext, "Observation");
    // Check if the schemas are equal
    assertThat(
        obsUtilAvroConverter.getSchema(), Matchers.equalTo(obsDirectAvroConverter.getSchema()));
  }

  @Test
  public void testForAvroRecords() throws URISyntaxException, IOException {

    IBaseResource baseResource = loadResource("patient_us_core.json", Patient.class);
    GenericRecord avroRecord = AvroUtil.convertToAvro((Resource) baseResource, fhirContext);

    AvroConverter avroConverter = AvroUtil.getConverter("Patient", fhirContext);
    IBaseResource baseResource1 = avroConverter.avroToResource(avroRecord);

    IParser jsonParser = FhirContext.forR4().newJsonParser();
    String string1 = (jsonParser.encodeResourceToString(baseResource));
    String string2 = jsonParser.encodeResourceToString(baseResource1);

    assertThat(string1.equals(string2), Matchers.equalTo(Boolean.TRUE));
  }

  @Test
  public void generateRecordForPatient() throws IOException {
    IParser parser = fhirContext.newJsonParser();
    String patientBundle =
        Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    GenericRecord record =
        AvroUtil.convertToAvro(bundle.getEntry().get(0).getResource(), fhirContext);
    Collection<Object> addressList = (Collection<Object>) record.get("address");
    // TODO We need to fix this again; the root cause is the change in `id` type to System.String.
    // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55
    // assertThat(record.get("id"), equalTo("471be3bc-08c7-4d78-a4ab-1b3d044dae67"));
    assertThat(addressList.size(), Matchers.equalTo(1));
    Record address = (Record) addressList.iterator().next();
    assertThat((String) address.get("city"), Matchers.equalTo("Waterloo"));
  }

  @Test
  public void convertObservationWithBigDecimalValue() throws IOException {
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    GenericRecord record = AvroUtil.convertToAvro(observation, fhirContext);
    GenericData.Record valueRecord = (GenericData.Record) record.get("value");
    BigDecimal value = (BigDecimal) ((GenericData.Record) valueRecord.get("quantity")).get("value");
    assertThat(value.doubleValue(), closeTo(25, 0.001));
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
