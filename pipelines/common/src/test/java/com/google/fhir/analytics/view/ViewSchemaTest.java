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
package com.google.fhir.analytics.view;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicator.FlatRow;
import com.google.fhir.analytics.view.ViewApplicator.RowElement;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ViewSchemaTest {

  private static final Logger log = LoggerFactory.getLogger(ViewSchemaTest.class);

  private static final String PATIENT_COLLECTION_VIEW =
      """
      {
        "name":  "new_pat",
        "resource": "Patient",
        "status": "active",
        "select": [
          {
            "column": [
              { "name": "id", "path": "id", "type": "id" },
              { "name": "last_name", "path": "name.family", "type": "string", "collection": true },
              { "name": "first_name", "path": "name.given", "type": "string", "collection": true }
            ]
          }
        ]
      }""";

  private ViewDefinition loadDefinition(String viewFile) throws IOException {
    String viewJson = Resources.toString(Resources.getResource(viewFile), StandardCharsets.UTF_8);
    ViewDefinition viewDef;
    try {
      viewDef = ViewDefinition.createFromString(viewJson);
    } catch (ViewDefinitionException e) {
      log.error("View validation for file {} failed with ", viewFile, e);
      throw new IllegalArgumentException("Failed to validate the view in " + viewFile);
    }
    return viewDef;
  }

  private <T extends IBaseResource> IBaseResource loadResource(
      String resourceFile, Class<T> resourceType) throws IOException {
    IParser jsonParser = FhirContext.forR4().newJsonParser();
    try (InputStream patientStream =
        getClass().getClassLoader().getResourceAsStream(resourceFile)) {
      return jsonParser.parseResource(resourceType, patientStream);
    }
  }

  private <T extends IBaseResource> RowList applyViewOnResource(
      String viewFile, String resourceFile, Class<T> resourceType)
      throws IOException, ViewApplicationException, ViewDefinitionException {
    String viewJson = Resources.toString(Resources.getResource(viewFile), StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    IBaseResource resource = loadResource(resourceFile, resourceType);
    ViewApplicator applicator = new ViewApplicator(viewDef);
    return applicator.apply(resource);
  }

  @Test
  public void schemaConversionPatient() throws IOException, ViewDefinitionException {
    ViewDefinition vDef = loadDefinition("patient_flat_view.json");
    Schema schema = ViewSchema.getAvroSchema(vDef);

    assertThat(schema.getField("pat_id").toString(), notNullValue());
    assertThat(schema.getField("active").toString(), notNullValue());
    assertThat(schema.getField("gender").toString(), notNullValue());
    assertThat(schema.getField("deceased").toString(), notNullValue());
    assertThat(schema.getField("organization_id").toString(), notNullValue());
    assertThat(schema.getField("practitioner_id").toString(), notNullValue());
    assertThat(schema.getField("family").toString(), notNullValue());
    assertThat(schema.getField("given").toString(), notNullValue());
  }

  @Test
  public void schemaConversionPatientWithCollection() throws ViewDefinitionException {
    ViewDefinition viewDef = ViewDefinition.createFromString(PATIENT_COLLECTION_VIEW);

    Schema schema = ViewSchema.getAvroSchema(viewDef);
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("last_name").toString(), notNullValue());
    assertThat(schema.getField("first_name").toString(), notNullValue());
  }

  @Test
  public void schemaConversionObservation() throws IOException, ViewDefinitionException {
    ViewDefinition vDef = loadDefinition("observation_flat_view.json");
    Schema schema = ViewSchema.getAvroSchema(vDef);

    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("status").toString(), notNullValue());
    assertThat(schema.getField("val_code").toString(), notNullValue());
    assertThat(schema.getField("code_sys").toString(), notNullValue());
    assertThat(schema.getField("patient_id").toString(), notNullValue());
    assertThat(schema.getField("encounter_id").toString(), notNullValue());
    assertThat(schema.getField("code").toString(), notNullValue());
    assertThat(schema.getField("val_sys").toString(), notNullValue());
    assertThat(schema.getField("val_quantity").toString(), notNullValue());
    assertThat(schema.getField("obs_date").toString(), notNullValue());
  }

  @Test
  public void setValueInRecordPatientTest()
      throws IOException, ViewApplicationException, ViewDefinitionException {
    ViewDefinition vDef = loadDefinition("patient_flat_view.json");
    String[] colNames = vDef.getAllColumns().keySet().toArray(String[]::new);

    RowList rows =
        applyViewOnResource(
            "patient_flat_view.json", "patient_with_practitioner.json", Patient.class);

    List<GenericRecord> result = ViewSchema.setValueInRecord(rows, vDef);

    GenericRecord record = result.get(0);
    FlatRow row = rows.getRows().get(0);
    List<RowElement> r1 = row.getElements();

    String val0 = r1.get(0).getString();
    String val1 = null;
    String val2 = "female";
    String val3 = null;
    String val4 = null;
    String val5 = r1.get(5).getString();
    String val6 = "Emily";
    String val7 = "Stevenson";

    assertThat(record.get(colNames[0]), equalTo(val0));
    assertThat(record.get(colNames[1]), equalTo(val1));
    assertThat(record.get(colNames[2]), equalTo(val2));
    assertThat(record.get(colNames[3]), equalTo(val3));
    assertThat(record.get(colNames[4]), equalTo(val4));
    assertThat(record.get(colNames[5]), equalTo(val5));
    assertThat(record.get(colNames[6]), equalTo(val6));
    assertThat(record.get(colNames[7]), equalTo(val7));
  }

  @Test
  public void setValueInRecordObservationTest()
      throws IOException, ViewApplicationException, ViewDefinitionException {
    ViewDefinition vDef = loadDefinition("observation_flat_view.json");
    String[] colNames = vDef.getAllColumns().keySet().toArray(String[]::new);

    RowList rows =
        applyViewOnResource(
            "observation_flat_view.json", "observation_codeable_concept.json", Observation.class);

    List<GenericRecord> result = ViewSchema.setValueInRecord(rows, vDef);

    GenericRecord record = result.get(0);
    FlatRow row = rows.getRows().get(0);
    List<RowElement> r1 = row.getElements();

    String val0 = r1.get(0).getString();
    String val1 = "7_SOME_PATIENT_REF";
    String val2 = r1.get(2).getString();
    String val3 = "final";
    String val4 = r1.get(4).getString();
    String val5 = null;
    String val6 = "a898c87a-1350-11df-a1f1-0026b9348838";
    String val7 = null;
    String val8 = "VAL_CODE1";
    String val9 = "VAL_SYS1";

    assertThat(record.get(colNames[0]), equalTo(val0));
    assertThat(record.get(colNames[1]), equalTo(val1));
    assertThat(record.get(colNames[2]), equalTo(val2));
    assertThat(record.get(colNames[3]), equalTo(val3));
    assertThat(record.get(colNames[4]), equalTo(val4));
    assertThat(record.get(colNames[5]), equalTo(val5));
    assertThat(record.get(colNames[6]), equalTo(val6));
    assertThat(record.get(colNames[7]), equalTo(val7));
    assertThat(record.get(colNames[8]), equalTo(val8));
    assertThat(record.get(colNames[9]), equalTo(val9));
  }

  /**
   * Tests the setValueInRecord method for a Patient View that handles Collections
   *
   * @see com.google.fhir.analytics.view.ViewSchema#setValueInRecord(RowList, ViewDefinition)
   */
  @Test
  public void setValueInRecordCollectionTest()
      throws IOException, ViewApplicationException, ViewDefinitionException {

    ViewDefinition viewDef = ViewDefinition.createFromString(PATIENT_COLLECTION_VIEW);
    String[] colNames = viewDef.getAllColumns().keySet().toArray(String[]::new);

    IBaseResource resource = loadResource("patient_child_us_core_profile.json", Patient.class);
    ViewApplicator applicator = new ViewApplicator(viewDef);
    RowList rows = applicator.apply(resource);

    List<GenericRecord> result = ViewSchema.setValueInRecord(rows, viewDef);

    GenericRecord record = result.get(0);
    FlatRow row = rows.getRows().get(0);
    List<RowElement> r1 = row.getElements();

    IParser iBaseParser = FhirContext.forR4().newJsonParser();

    String val0 = r1.get(0).getString();
    String val1 = iBaseParser.encodeToString(r1.get(1).getValues().get(0));
    String val2 = iBaseParser.encodeToString(r1.get(2).getValues().get(0));

    assertThat(record.get(colNames[0]), equalTo(val0));
    assertThat(record.get(colNames[1]), equalTo(new String[] {val1}));
    assertThat(record.get(colNames[2]), equalTo(new String[] {val2}));
  }
}
