/*
 * Copyright 2020-2025 Google LLC
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.util.List;
import java.util.Objects;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ViewDefinitionTest {

  @Test
  public void createFromJson() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_addresses_view.json"), StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("patient_addresses"));

    List<ViewDefinition.Select> selects = viewDef.getSelect();
    assertThat(selects, notNullValue());

    assertThat(viewDef.getSelect().size(), equalTo(2));

    assertThat(viewDef.getSelect().get(0).getColumn(), notNullValue());

    assertThat(viewDef.getSelect().get(0).getColumn().size(), equalTo(2));
    List<ViewDefinition.Column> columnOne = viewDef.getSelect().get(1).getColumn();
    assertThat(columnOne, notNullValue());
    assertThat(columnOne.size(), equalTo(4));
    assertThat(viewDef.getSelect().get(1).getColumn().get(1).getName(), equalTo("use"));
    assertThat(viewDef.getSelect().get(1).getColumn().get(3).getPath(), equalTo("postalCode"));
  }

  @Test
  public void createFromJsonUnion() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_address_and_contact_union_view.json"),
            StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("patient_and_contact_addresses"));
    assertThat(viewDef.getSelect().size(), equalTo(1));
    assertThat(viewDef.getSelect().get(0).getColumn().size(), equalTo(1));
    assertThat(viewDef.getSelect().get(0).getUnionAll().size(), equalTo(2));
    assertThat(viewDef.getSelect().get(0).getUnionAll().get(0).getColumn().size(), equalTo(4));
    assertThat(Objects.requireNonNull(viewDef.getSelect().get(0).getUnionAll()).size(), equalTo(2));
    assertThat(
        Objects.requireNonNull(viewDef.getSelect().get(0).getUnionAll().get(0).getColumn()).size(),
        equalTo(4));
    assertThat(
        viewDef.getSelect().get(0).getUnionAll().get(0).getColumn().get(1).getName(),
        equalTo("city"));
    ImmutableMap<String, JDBCType> schema = ViewSchema.getDbSchema(viewDef);
    assertThat(
        schema.keySet(),
        equalTo(Sets.newHashSet("patient_id", "street", "city", "zip", "is_patient")));
    // There is no type definitions in the above view and since we don't have automatic type
    // derivation yet, all types will be string.
    assertThat(schema.get("patient_id"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("street"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("city"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("zip"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("is_patient"), equalTo(JDBCType.VARCHAR));
  }

  @Test
  public void createFromJsonUnionWithTypes() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_address_and_contact_union_with_types_view.json"),
            StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("patient_and_contact_addresses_with_types"));
    ImmutableMap<String, JDBCType> schema = ViewSchema.getDbSchema(viewDef);
    assertThat(
        schema.keySet(),
        equalTo(
            Sets.newHashSet(
                "patient_id",
                "birth_date",
                "multiple_birth",
                "street",
                "city",
                "zip",
                "address_use",
                "period_end",
                "is_patient")));
    assertThat(schema.get("patient_id"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("street"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("city"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("zip"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("is_patient"), equalTo(JDBCType.BOOLEAN));
    assertThat(schema.get("address_use"), equalTo(JDBCType.VARCHAR));
    assertThat(schema.get("period_end"), equalTo(JDBCType.TIMESTAMP));
    assertThat(schema.get("birth_date"), equalTo(JDBCType.DATE));
    assertThat(schema.get("multiple_birth"), equalTo(JDBCType.INTEGER));
  }

  @Test
  public void createFromJsonWhereAndConstant() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("us_core_blood_pressures_view.json"), StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("us_core_blood_pressures"));
    assertThat(viewDef.getSelect().size(), equalTo(3));
    assertThat(viewDef.getWhere().size(), equalTo(1));
    assertThat(viewDef.getSelect().get(1).getColumn().size(), equalTo(4));
    assertThat(Objects.requireNonNull(viewDef.getWhere()).size(), equalTo(1));
    assertThat(Objects.requireNonNull(viewDef.getSelect().get(1).getColumn()).size(), equalTo(4));
    // Check constant replacements
    assertThat(
        viewDef.getWhere().get(0).getPath(),
        equalTo("code.coding.exists(system='http://loinc.org' and code='85354-9')"));
    assertThat(
        viewDef.getSelect().get(1).getForEach(),
        equalTo(
            "component.where(code.coding.exists(system='http://loinc.org' and"
                + " code='8480-6')).first()"));
    assertThat(
        viewDef.getSelect().get(2).getForEach(),
        equalTo(
            "component.where(code.coding.exists(system='http://loinc.org' and"
                + " code='8462-4')).first()"));
  }

  @Test(expected = ViewDefinitionException.class)
  public void inconsistentSelects() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_inconsistent_selects_view.json"),
            StandardCharsets.UTF_8);
    ViewDefinition.createFromString(viewJson);
  }

  @Test(expected = ViewDefinitionException.class)
  public void inconsistentUnion() throws IOException, ViewDefinitionException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_inconsistent_union_view.json"), StandardCharsets.UTF_8);
    ViewDefinition.createFromString(viewJson);
  }

  @Test(expected = ViewDefinitionException.class)
  public void emptyResource() throws ViewDefinitionException {
    ViewDefinition.createFromString(
        """
    {
      "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
      "select": [
        {
          "column": [
            {
              "path": "getResourceKey()",
              "name": "patient_id"
            }
          ]
        }
      ],
      "status": "draft"
    }
    """);
  }

  @Test(expected = ViewDefinitionException.class)
  public void emptyColumnName() throws ViewDefinitionException {
    ViewDefinition.createFromString(
        """
    {
      "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
      "select": [
        {
          "column": [
            {
              "path": "getResourceKey()"
            }
          ]
        }
      ],
      "status": "draft",
      "resource": "Patient"
    }
    """);
  }

  @Test(expected = ViewDefinitionException.class)
  public void emptyColumnPath() throws ViewDefinitionException {
    ViewDefinition.createFromString(
        """
    {
      "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
      "select": [
        {
          "column": [
            {
              "name": "patient_id"
            }
          ]
        }
      ],
      "status": "draft",
      "resource": "Patient"
    }
    """);
  }

  @Test(expected = ViewDefinitionException.class)
  public void undefinedConstant() throws ViewDefinitionException {
    ViewDefinition.createFromString(
        """
    {
      "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
      "select": [
        {
          "column": [
            {
              "path": "code.coding.exists(code=%a_constant).first()",
              "name": "code"
            }
          ]
        }
      ],
      "status": "draft",
      "resource": "Observation"
    }
    """);
  }

  @Test(expected = ViewDefinitionException.class)
  public void badColumnName() throws ViewDefinitionException {
    ViewDefinition.createFromString(
        """
    {
      "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
      "select": [
        {
          "column": [
            {
              "path": "code.coding.exists(code='test').first()",
              "name": "code-12"
            }
          ]
        }
      ],
      "status": "draft",
      "resource": "Observation"
    }
    """);
  }
}
