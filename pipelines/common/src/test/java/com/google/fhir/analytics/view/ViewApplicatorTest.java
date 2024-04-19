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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Immunization;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class ViewApplicatorTest {

  private static final Logger log = LoggerFactory.getLogger(ViewApplicatorTest.class);

  private ViewDefinition loadDefinition(String viewFile) throws IOException {
    String viewJson = Resources.toString(Resources.getResource(viewFile), StandardCharsets.UTF_8);
    ViewDefinition viewDef;
    try {
      viewDef = ViewDefinition.createFromString(viewJson);
    } catch (ViewDefinitionException e) {
      // This is just for convenience, in production code this exception should be properly handled.
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
      throws IOException, ViewApplicationException {
    ViewDefinition viewDef = loadDefinition(viewFile);
    IBaseResource resource = loadResource(resourceFile, resourceType);
    ViewApplicator applicator = new ViewApplicator(viewDef);
    return applicator.apply(resource);
  }

  @Test
  public void emptyForEach() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource("patient_addresses_view.json", "patient.json", Patient.class);
    assertTrue(rows.isEmpty());
  }

  @Test
  public void forEach() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_addresses_view.json", "patient_with_address.json", Patient.class);
    assertThat(rows.getRows().size(), equalTo(2));
    assertThat(rows.getRows().get(1).getElements().get(0).getName(), equalTo("patient_id"));
    assertFalse(rows.getRows().get(1).getElements().get(0).isCollection());
    assertThat(rows.getRows().get(1).getElements().get(0).getString(), equalTo("12345"));
    assertFalse(rows.getRows().get(1).getElements().get(1).isCollection());
    assertThat(rows.getRows().get(1).getElements().get(1).getName(), equalTo("multiple_birth"));
    assertThat(rows.getRows().get(1).getElements().get(1).getSingleValue(), equalTo(null));
    assertThat(rows.getRows().get(1).getElements().get(2).getName(), equalTo("street"));
    assertThat(
        rows.getRows().get(1).getElements().get(2).getPrimitive(), equalTo("10\nParliament st."));
  }

  @Test
  public void emptyForEachOrNullWithUnion() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource("patient_multiple_foreach_view.json", "patient.json", Patient.class);
    assertFalse(rows.isEmpty());
    // Note from `unionAll` we get two null rows since there are two selects with `forEachOrNull`.
    assertThat(rows.getRows().size(), equalTo(2));
    assertThat(
        rows.getColumnInfos().keySet(),
        equalTo(
            Set.of(
                "patient_id",
                "family",
                "street_nested",
                "city_nested",
                "languages",
                "street",
                "city",
                "is_patient")));
  }

  // TODO add a test for incompatible schema

  @Test
  public void multipleForEachOrNull() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_multiple_foreach_view.json", "patient_with_address.json", Patient.class);
    assertThat(rows.getRows().size(), equalTo(8));
    assertThat(rows.getRows().get(3).getElements().get(0).getName(), equalTo("patient_id"));
    assertThat(rows.getRows().get(3).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(6).getElements().get(4).getName(), equalTo("languages"));
    assertThat(rows.getRows().get(6).getElements().get(4).getSingleValue(), equalTo(null));
    assertThat(rows.getRows().get(6).getElements().get(5).getName(), equalTo("street"));
    assertThat(
        rows.getRows().get(6).getElements().get(5).getPrimitive(), equalTo("250\nContact1 st."));
    assertThat(rows.getRows().get(6).getElements().get(3).getName(), equalTo("city_nested"));
    assertThat(rows.getRows().get(6).getElements().get(3).getPrimitive(), equalTo("Kitchener"));
  }

  @Test
  public void unionWithForEach() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_address_and_contact_union_view.json",
            "patient_with_address.json",
            Patient.class);
    assertThat(rows.getRows().size(), equalTo(4));
    assertThat(rows.getRows().get(1).getElements().get(0).getName(), equalTo("patient_id"));
    assertThat(rows.getRows().get(1).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(1).getElements().get(1).getName(), equalTo("street"));
    assertThat(
        rows.getRows().get(1).getElements().get(1).getPrimitive(), equalTo("10\nParliament st."));
    assertThat(
        rows.getRows().get(3).getElements().get(1).getPrimitive(), equalTo("15\nContact2 st."));
  }

  @Test
  public void getReferenceKey() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "observation_patient_id_view.json", "observation_decimal.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(1));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getString(), equalTo("obs1"));
    assertThat(rows.getRows().get(0).getElements().get(1).getName(), equalTo("patient_id"));
    assertThat(
        rows.getRows().get(0).getElements().get(1).getString(), equalTo("7_SOME_PATIENT_REF"));
    assertThat(
        rows.getRows().get(0).getElements().get(2).getName(), equalTo("effective_date_time"));
    assertThat(
        rows.getRows().get(0).getElements().get(2).getPrimitive(),
        equalTo(new DateTime("2021-04-16T11:12:33+03:00").toDate()));
    assertThat(rows.getRows().get(0).getElements().get(3).getName(), equalTo("value_quantity"));
    assertThat(
        rows.getRows().get(0).getElements().get(3).getPrimitive(),
        closeTo(BigDecimal.valueOf(25.0), BigDecimal.valueOf(0.001)));
  }

  @Test
  public void getReferenceKeyNoSubject() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "observation_patient_id_view.json", "observation_no_subject.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(0));
  }

  @Test
  public void getReferenceForEach() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_practitioner_id_view.json", "patient_with_practitioner.json", Patient.class);
    assertThat(rows.getRows().size(), equalTo(3));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(0).getElements().get(1).getName(), equalTo("practitioner_id"));
    assertThat(rows.getRows().get(0).getElements().get(1).getString(), equalTo("prac1"));
    assertThat(rows.getRows().get(1).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(1).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(1).getElements().get(1).getName(), equalTo("practitioner_id"));
    // The second `generalPractitioner` is of type `Organization` hence it should not match
    // `getReferenceKey('Practitioner')`.
    assertThat(rows.getRows().get(1).getElements().get(1).getString(), equalTo(null));
    assertThat(rows.getRows().get(2).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(2).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(2).getElements().get(1).getName(), equalTo("practitioner_id"));
    assertThat(rows.getRows().get(2).getElements().get(1).getString(), equalTo("prac2"));
  }

  @Test
  public void constTest() throws IOException, ViewApplicationException, ParseException {
    RowList rows =
        applyViewOnResource(
            "observation_many_constants_view.json", "observation_decimal.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(1));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getString(), equalTo("obs1"));
    assertThat(
        rows.getRows().get(0).getElements().get(1).getName(), equalTo("effective_date_time"));
    assertThat(
        rows.getRows().get(0).getElements().get(1).getPrimitive(),
        equalTo(new DateTime("2021-04-16T11:12:33+03:00").toDate()));
  }

  @Test
  public void constDateNoMatch() throws IOException, ViewApplicationException {
    ViewDefinition viewDef = loadDefinition("observation_many_constants_view.json");
    Observation obs = (Observation) loadResource("observation_decimal.json", Observation.class);
    obs.setEffective(new DateTimeType("2020-01-01"));
    ViewApplicator applicator = new ViewApplicator(viewDef);
    RowList rows = applicator.apply(obs);
    assertThat(rows.getRows().size(), equalTo(0));
  }

  @Test
  public void whereAndConstantNoMatch() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "us_core_blood_pressures_view.json", "observation_decimal.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(0));
  }

  @Test
  public void getPrimitives() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_flat_view.json", "patient_with_practitioner.json", Patient.class);
    assertThat(rows.getRows().size(), equalTo(3));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("pat_id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getString(), equalTo("12345"));
    assertThat(rows.getRows().get(0).getElements().get(2).getName(), equalTo("gender"));
    assertThat(rows.getRows().get(0).getElements().get(2).getString(), equalTo("female"));
  }

  @Test
  public void toCsv() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_flat_view.json", "patient_with_practitioner.json", Patient.class);
    StringBuilder expected = new StringBuilder();
    expected
        .append("pat_id,active,gender,deceased,organization_id,practitioner_id,family,given\n")
        .append("12345,null,female,null,null,prac1,Emily,Stevenson\n")
        .append("12345,null,female,null,null,null,Emily,Stevenson\n")
        .append("12345,null,female,null,null,prac2,Emily,Stevenson");
    assertThat(rows.toCsv(), equalTo(expected.toString()));
  }

  @Test
  public void toHtml() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "patient_flat_view.json", "patient_with_practitioner.json", Patient.class);
    StringBuilder expected = new StringBuilder();
    expected
        .append("<thead>")
        .append("<tr><td>pat_id</td><td>active</td><td>gender</td><td>deceased</td>")
        .append("<td>organization_id</td><td>practitioner_id</td><td>family</td><td>given</td>")
        .append("</tr></thead><tbody>")
        .append("<tr><td>12345</td><td>null</td><td>female</td><td>null</td><td>null</td>")
        .append("<td>prac1</td><td>Emily</td><td>Stevenson</td></tr>")
        .append("<tr><td>12345</td><td>null</td><td>female</td><td>null</td><td>null</td>")
        .append("<td>null</td><td>Emily</td><td>Stevenson</td></tr>")
        .append("<tr><td>12345</td><td>null</td><td>female</td><td>null</td><td>null</td>")
        .append("<td>prac2</td><td>Emily</td><td>Stevenson</td></tr></tbody>");
    assertThat(rows.toHtml(), equalTo(expected.toString()));
  }

  @Test
  public void encounterFlat() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource("encounter_flat_view.json", "encounter.json", Encounter.class);
    assertThat(rows.getRows().size(), equalTo(3));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(
        rows.getRows().get(0).getElements().get(0).getString(),
        equalTo("46f919e2-2d3c-c055-e97b-f3ad7bbf110e"));
    assertThat(rows.getRows().get(2).getElements().get(3).getName(), equalTo("service_org_id"));
    assertThat(rows.getRows().get(2).getElements().get(3).getString(), equalTo("1"));
  }

  @Test
  public void observationFlat() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource(
            "observation_flat_view.json", "observation_codeable_concept.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(2));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getString(), equalTo("obs1"));
    assertThat(rows.getRows().get(0).getElements().get(9).getName(), equalTo("val_sys"));
    assertThat(rows.getRows().get(0).getElements().get(9).getPrimitive(), equalTo("VAL_SYS1"));
    assertThat(rows.getRows().get(1).getElements().get(8).getName(), equalTo("val_code"));
    assertThat(rows.getRows().get(1).getElements().get(8).getPrimitive(), equalTo("VAL_CODE2"));
  }

  @Test
  public void conditionFlat() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource("condition_flat_view.json", "condition.json", Condition.class);
    assertThat(rows.getRows().size(), equalTo(4));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(
        rows.getRows().get(0).getElements().get(0).getString(),
        equalTo("610e4380-aa0d-4d86-0104-479010ef0471"));
    assertThat(rows.getRows().get(2).getElements().get(7).getName(), equalTo("clinical_status"));
    assertThat(rows.getRows().get(2).getElements().get(7).getPrimitive(), equalTo("active"));
  }

  @Test
  public void immunizationFlat() throws IOException, ViewApplicationException {
    RowList rows =
        applyViewOnResource("imminization_flat_view.json", "immunization.json", Immunization.class);
    assertThat(rows.getRows().size(), equalTo(2));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(
        rows.getRows().get(0).getElements().get(0).getString(),
        equalTo("8db297e1-658f-6612-0e3a-7f9c0943b877"));
    assertThat(rows.getRows().get(0).getElements().get(9).getName(), equalTo("practitioner_id"));
    assertThat(rows.getRows().get(0).getElements().get(9).getString(), equalTo("9999999899"));
    assertThat(rows.getRows().get(1).getElements().get(10).getName(), equalTo("organization_id"));
    assertThat(rows.getRows().get(1).getElements().get(10).getString(), equalTo("199"));
  }
}
