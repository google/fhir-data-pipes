/*
 * Copyright 2020-2023 Google LLC
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
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ViewApplicatorTest {

  private ViewDefinition loadDefinition(String viewFile) throws IOException {
    String viewJson = Resources.toString(Resources.getResource(viewFile), StandardCharsets.UTF_8);
    ViewDefinition viewDef;
    try {
      viewDef = ViewDefinition.createFromString(viewJson);
    } catch (ViewDefinitionException e) {
      // This is just for convenience, in production code this exception should be properly handled.
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
    assertThat(rows.getRows().get(1).getElements().get(0).getSingleIdPart(), equalTo("12345"));
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
    assertThat(rows.getRows().get(3).getElements().get(0).getSingleIdPart(), equalTo("12345"));
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
    assertThat(rows.getRows().get(1).getElements().get(0).getSingleIdPart(), equalTo("12345"));
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
    assertThat(rows.getRows().get(0).getElements().get(0).getSingleIdPart(), equalTo("obs1"));
    assertThat(rows.getRows().get(0).getElements().get(1).getName(), equalTo("patient_id"));
    assertThat(
        rows.getRows().get(0).getElements().get(1).getSingleIdPart(),
        equalTo("7_SOME_PATIENT_REF"));
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
    assertThat(rows.getRows().get(0).getElements().get(0).getSingleIdPart(), equalTo("12345"));
    assertThat(rows.getRows().get(0).getElements().get(1).getName(), equalTo("practitioner_id"));
    assertThat(rows.getRows().get(0).getElements().get(1).getSingleIdPart(), equalTo("prac1"));
    assertThat(rows.getRows().get(1).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(1).getElements().get(0).getSingleIdPart(), equalTo("12345"));
    assertThat(rows.getRows().get(1).getElements().get(1).getName(), equalTo("practitioner_id"));
    // The second `generalPractitioner` is of type `Organization` hence it should not match
    // `getReferenceKey('Practitioner')`.
    assertThat(rows.getRows().get(1).getElements().get(1).getSingleIdPart(), equalTo(null));
    assertThat(rows.getRows().get(2).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(2).getElements().get(0).getSingleIdPart(), equalTo("12345"));
    assertThat(rows.getRows().get(2).getElements().get(1).getName(), equalTo("practitioner_id"));
    assertThat(rows.getRows().get(2).getElements().get(1).getSingleIdPart(), equalTo("prac2"));
  }

  @Test
  public void constTest() throws IOException, ViewApplicationException, ParseException {
    RowList rows =
        applyViewOnResource(
            "observation_many_constants_view.json", "observation_decimal.json", Observation.class);
    assertThat(rows.getRows().size(), equalTo(1));
    assertThat(rows.getRows().get(0).getElements().get(0).getName(), equalTo("id"));
    assertThat(rows.getRows().get(0).getElements().get(0).getSingleIdPart(), equalTo("obs1"));
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
    assertThat(rows.getRows().get(0).getElements().get(0).getSingleIdPart(), equalTo("12345"));
    assertThat(rows.getRows().get(0).getElements().get(2).getName(), equalTo("gender"));
    assertThat(rows.getRows().get(0).getElements().get(2).getString(), equalTo("female"));
  }
}
