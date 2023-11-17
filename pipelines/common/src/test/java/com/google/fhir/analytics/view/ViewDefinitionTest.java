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
import static org.hamcrest.Matchers.equalTo;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ViewDefinitionTest {

  @Test
  public void createFromJson() throws IOException {
    String viewJson =
        Resources.toString(
            Resources.getResource("patient_addresses_view.json"), StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("patient_addresses"));
    assertThat(viewDef.getSelect().size(), equalTo(2));
    assertThat(viewDef.getSelect().get(0).getColumn().size(), equalTo(1));
    assertThat(viewDef.getSelect().get(1).getColumn().size(), equalTo(4));
    assertThat(viewDef.getSelect().get(1).getColumn().get(1).getName(), equalTo("use"));
    assertThat(viewDef.getSelect().get(1).getColumn().get(3).getPath(), equalTo("postalCode"));
  }

  @Test
  public void createFromJsonUnion() throws IOException {
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
    assertThat(
        viewDef.getSelect().get(0).getUnionAll().get(0).getColumn().get(1).getName(),
        equalTo("city"));
  }

  @Test
  public void createFromJsonWhereAndConstant() throws IOException {
    String viewJson =
        Resources.toString(
            Resources.getResource("us_core_blood_pressures_view.json"), StandardCharsets.UTF_8);
    ViewDefinition viewDef = ViewDefinition.createFromString(viewJson);
    assertThat(viewDef.getName(), equalTo("us_core_blood_pressures"));
    assertThat(viewDef.getSelect().size(), equalTo(3));
    assertThat(viewDef.getWhere().size(), equalTo(1));
    assertThat(viewDef.getConstant().size(), equalTo(3));
    assertThat(viewDef.getSelect().get(1).getColumn().size(), equalTo(4));
    assertThat(viewDef.getConstant().get(1).getValueCode(), equalTo("8462-4"));
    assertThat(viewDef.getConstant().get(1).getValueBoolean(), equalTo(null));
    assertThat(
        viewDef.getWhere().get(0).getPath(),
        equalTo("code.coding.exists(system='http://loinc.org' and code=%bp_code)"));
  }
}
