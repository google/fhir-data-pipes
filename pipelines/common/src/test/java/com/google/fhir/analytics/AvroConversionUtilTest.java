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
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroConversionUtilTest {
  private AvroConversionUtil instance = AvroConversionUtil.getInstance();

  private FhirContext fhirContext;
  private String patientBundle;
  private String observationBundle;

  @Before
  public void setup() throws IOException {
    AvroConversionUtil.initializeAvroConverters();
    patientBundle =
        Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
    observationBundle =
        Resources.toString(
            Resources.getResource("observation_bundle.json"), StandardCharsets.UTF_8);
    this.fhirContext = FhirContext.forR4Cached();
  }

  @Test
  public void getResourceSchema_Patient() {
    Schema schema = instance.getResourceSchema("Patient", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("name").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Observation() {
    Schema schema = instance.getResourceSchema("Observation", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("basedOn").toString(), notNullValue());
    assertThat(schema.getField("subject").toString(), notNullValue());
    assertThat(schema.getField("code").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Encounter() {
    Schema schema = instance.getResourceSchema("Encounter", fhirContext);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("status").toString(), notNullValue());
    assertThat(schema.getField("class").toString(), notNullValue());
  }

  @Test
  public void generateRecords_BundleOfPatients() {
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
  public void generateRecords_BundleOfObservations() {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    List<GenericRecord> recordList = instance.generateRecords(bundle, fhirContext);
    assertThat(recordList.size(), equalTo(6));
  }

  @Test
  public void generateRecordForPatient() {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    GenericRecord record =
        AvroConversionUtil.getInstance()
            .convertToAvro(bundle.getEntry().get(0).getResource(), fhirContext);
    Collection<Object> addressList = (Collection<Object>) record.get("address");
    // TODO We need to fix this again; the root cause is the change in `id` type to System.String.
    // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55
    // assertThat(record.get("id"), equalTo("471be3bc-08c7-4d78-a4ab-1b3d044dae67"));
    assertThat(addressList.size(), equalTo(1));
    Record address = (Record) addressList.iterator().next();
    assertThat((String) address.get("city"), equalTo("Waterloo"));
  }

  @Test
  public void convertObservationWithBigDecimalValue() throws IOException {
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    GenericRecord record = AvroConversionUtil.getInstance().convertToAvro(observation, fhirContext);
    GenericData.Record valueRecord = (GenericData.Record) record.get("value");
    Double value = (Double) ((GenericData.Record) valueRecord.get("quantity")).get("value");
    assertThat(value, closeTo(25, 0.001));
  }
}
