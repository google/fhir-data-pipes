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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicator.FlatRow;
import com.google.fhir.analytics.view.ViewApplicator.RowElement;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import com.google.gson.Gson;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDecimalDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.hl7.fhir.r4.utils.FHIRLexer.FHIRLexerException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class SQLonFHIRv2Test {

  private static final Logger log = LoggerFactory.getLogger(SQLonFHIRv2Test.class);

  private static final Set<String> SKIPPED_TESTS =
      ImmutableSet.of(
          // Reason: The `join()` function of R4 FHIRPathEngine implementation requires a parameter.
          // In the FHIRPath spec `join()` may have zero arguments and this seems to be fixed in the
          // R5 implementation of FHIRPathEngine, so this might be a version issue:
          // https://build.fhir.org/ig/HL7/FHIRPath/#joinseparator-string--string
          // TODO report and/or fix this is the core FHIR implementation.
          "fn_join.join with no value - default to no separator",
          "fhirpath.string join: default separator",
          // Reason: `lowBoundry()` and `highBoundry()` functions when called with no precision
          // parameters (for `date` type), default to 8 as the precision which is incorrect.
          // TODO report and/or fix this is the core FHIR implementation.
          "fn_boundary.date lowBoundary",
          "fn_boundary.date highBoundary",

          // TODO the error condition here does not seem right.
          "validate.wrong type in forEach",

          // Reason: getResourceKey is used inside the FHIRPath which we don't support yet.
          // TODO: Implement the above feature.
          "fn_reference_keys.getReferenceKey result matches getResourceKey without type specifier",
          "fn_reference_keys.getReferenceKey result matches getResourceKey with right type"
              + " specifier",
          "fn_reference_keys.getReferenceKey result matches getResourceKey with wrong type"
              + " specifier");

  @Test
  public void runAllTests() throws IOException {
    // TODO make the FHIR version optional.
    IParser parser = FhirContext.forR4Cached().newJsonParser();
    String testsRoot = Resources.getResource("sql-on-fhir-v2-tests").getPath();
    Path testsPath = Paths.get(testsRoot);
    List<Path> testFiles =
        Files.walk(testsPath)
            .filter(f -> f.getFileName().toString().endsWith(".json"))
            .collect(Collectors.toList());
    for (Path p : testFiles) {
      InputStream stream = new FileInputStream(p.toFile());
      String jsonContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
      JSONObject json = new JSONObject(jsonContent);
      String collectionTitle = (String) json.get("title");
      log.info("Next test-collection: " + collectionTitle);
      List<IBaseResource> resources = new ArrayList<>();
      JSONArray resourceArray = (JSONArray) json.get("resources");
      for (Object r : resourceArray) {
        resources.add(parser.parseResource(r.toString()));
      }
      for (Object t : (JSONArray) json.get("tests")) {
        JSONObject testObj = (JSONObject) t;
        String testTitle = (String) testObj.get("title");
        if (SKIPPED_TESTS.contains(collectionTitle + "." + testTitle)) continue;
        // Note: To debug a single test case we can do the following:
        // if (!testTitle.equals("wrong type in forEach")) continue;
        log.info("Next test: " + testTitle);
        ExpectedRows expectedRows = null; // will be null if `expectError` is set.
        if (!testObj.has("expectError")) {
          expectedRows = new ExpectedRows((JSONArray) testObj.get("expect"));
        }
        try {
          ViewDefinition view =
              ViewDefinition.createFromString(testObj.get("view").toString(), false);
          ViewApplicator applicator = new ViewApplicator(view);
          int totalRows = 0;
          for (IBaseResource resource : resources) {
            if (!view.getResource().equals(resource.fhirType())) continue;
            RowList rowList = applicator.apply(resource);
            for (FlatRow row : rowList.getRows()) {
              assertThat("Row not found; index " + totalRows, expectedRows.hasRow(row));
              totalRows++;
            }
          }
          assertThat("No exceptions were thrown", expectedRows != null);
          assertThat(
              String.format(
                  "Number of rows does not match %d vs %d", totalRows, expectedRows.getNumRows()),
              totalRows == expectedRows.getNumRows());
        } catch (ViewApplicationException | ViewDefinitionException | FHIRLexerException e) {
          assertThat("Expected errors but no view exceptions was thrown!", expectedRows == null);
        }
      }
    }
  }

  private void findRow(FlatRow row, List<Map<String, Object>> expectedRows) {}

  private static class ExpectedRows {
    private final List<Map<String, Object>> rows;

    public ExpectedRows(JSONArray jsonArray) {
      rows = new ArrayList<>();
      for (Object r : jsonArray) {
        Map<String, Object> row = new HashMap<>();
        JSONObject jsonObject = (JSONObject) r;
        for (String key : jsonObject.keySet()) {
          row.put(key, jsonObject.get(key));
        }
        rows.add(row);
      }
    }

    public static ExpectedRows parse(String jsonStr) {
      Gson gson = new Gson();
      return gson.fromJson(jsonStr, ExpectedRows.class);
    }

    int getNumRows() {
      return rows.size();
    }

    public boolean hasRow(FlatRow row) {
      for (Map<String, Object> myRow : rows) {
        boolean matches = true;
        for (RowElement e : row.getElements()) {
          if (!myRow.containsKey(e.getName())) {
            matches = false;
            break;
          }
          Object myValue = myRow.get(e.getName());
          if (myValue instanceof JSONArray) {
            List<Object> myList = ((JSONArray) myValue).toList();
            List<IBase> otherList = e.getValues();
            if (myList.size() != otherList.size()) {
              matches = false;
              break;
            }
            for (int i = 0; i < myList.size(); i++) {
              if (!typeSafeMatch(myList.get(i), otherList.get(i))) {
                matches = false;
                break;
              }
            }
            if (!matches) break;
          } else {
            if (myValue.equals(JSONObject.NULL)) {
              if (e.getValues() != null && !e.getValues().isEmpty()) {
                matches = false;
                break;
              }
            } else {
              if (!typeSafeMatch(myValue, e.getSingleValue())) {
                matches = false;
                break;
              }
            }
          }
        }
        if (matches) return true;
      }
      log.warn("Row {} did not match any expected row.", row);
      return false;
    }

    private boolean typeSafeMatch(Object expected, IBase actual) {
      if (actual == null) return false;
      if (expected.equals(actual)) return true;
      // TODO add other types as required by tests.
      if (actual instanceof IPrimitiveType<?>) {
        if (actual instanceof IIdType) {
          return ((IIdType) actual).getValue().equals(expected);
        }
        if (actual instanceof IBaseDecimalDatatype) {
          BigDecimal expectedBigDecimal = null;
          if (expected instanceof Integer) {
            expectedBigDecimal = BigDecimal.valueOf((Integer) expected);
          }
          if (expected instanceof BigDecimal) {
            expectedBigDecimal = (BigDecimal) expected;
          }
          return (expectedBigDecimal != null)
              && ((IBaseDecimalDatatype) actual).getValue().compareTo(expectedBigDecimal) == 0;
        }
        Object value = ((IPrimitiveType<?>) actual).getValue();
        if (value.equals(expected)) return true;
        if (((IPrimitiveType<?>) actual).getValueAsString().equals(expected)) return true;
      }
      return false;
    }
  }
}
