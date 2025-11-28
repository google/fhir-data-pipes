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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicator.FlatRow;
import com.google.fhir.analytics.view.ViewApplicator.RowElement;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDecimalDatatype;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.jspecify.annotations.Nullable;
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
          "validate.wrong type in forEach");

  @SuppressWarnings("NullAway")
  @Test
  public void runAllTests() throws IOException {
    Map<String, AllTestResults> allTestResults = new LinkedHashMap<>();
    String testsRoot = Resources.getResource("sql-on-fhir-v2-tests").getPath();
    Path testsPath = Paths.get(testsRoot);
    List<Path> testFiles;
    try (Stream<Path> stream = Files.walk(testsPath)) {
      testFiles = stream.filter(f -> f.getFileName().toString().endsWith(".json")).toList();
    }
    Gson gson = new Gson();
    for (Path p : testFiles) {
      String jsonContent = "";
      try (InputStream stream = new FileInputStream(p.toFile())) {
        jsonContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
      }
      TestDef testDef = gson.fromJson(jsonContent, TestDef.class);
      List<String> fhirVersions = testDef.fhirVersion;
      if (fhirVersions == null || fhirVersions.isEmpty()) {
        // Use FHIR version R4 as the default.
        fhirVersions = List.of("4.0");
      }
      Map<String, Boolean> success = new LinkedHashMap<>();
      for (String fhirVersion : fhirVersions) {
        FhirVersionEnum version = ViewDefinition.convertFhirVersion(fhirVersion);
        IParser parser = FhirContext.forCached(version).newJsonParser();
        log.info("Next test-collection: {} fhirVersion: {}", testDef.title, fhirVersion);
        List<IBaseResource> resources = new ArrayList<>();
        for (JsonObject r : testDef.resources) {
          resources.add(parser.parseResource(r.toString()));
        }
        for (SingleTest test : testDef.tests) {
          if (SKIPPED_TESTS.contains(testDef.title + "." + test.title)) {
            success.put(testDef.title, false);
            continue;
          }
          // Note: To debug a single test case we can do the following:
          // if (!test.title.equals("basic attribute")) continue;
          log.info("Next test: " + test.title);
          ExpectedRows expectedRows = null; // will be null if `expectError` is set.
          if (test.expectError == null || !test.expectError) {
            expectedRows = new ExpectedRows(test.expect);
          }
          try {
            test.view.validateAndSetUp(false, fhirVersion);
            ViewApplicator applicator = new ViewApplicator(test.view);
            int totalRows = 0;
            for (IBaseResource resource : resources) {
              if (test.view.getResource() != null
                  && !test.view.getResource().equals(resource.fhirType())) continue;
              RowList rowList = applicator.apply(resource);
              for (FlatRow row : rowList.getRows()) {
                assertThat(
                    "Row not found; index " + totalRows,
                    expectedRows != null && expectedRows.hasRow(row));
                totalRows++;
              }
            }
            assertThat("No exceptions were thrown", expectedRows != null);
            assertThat(
                String.format(
                    "Number of rows does not match %d vs %d",
                    totalRows, expectedRows != null ? expectedRows.getNumRows() : 0),
                totalRows == expectedRows.getNumRows());
          } catch (ViewApplicationException | ViewDefinitionException e) {
            assertThat(
                "View exceptions were thrown while none was expected!", expectedRows == null);
          }
          // This is not really accurate as we should report success/failure per FHIR version.
          success.put(test.title, true);
        }
      }
      List<SingleTestResult> resultList = new ArrayList<>();
      for (Entry<String, Boolean> kv : success.entrySet()) {
        if (kv.getValue()) {
          resultList.add(new SingleTestResult(kv.getKey(), true));
        } else {
          resultList.add(new SingleTestResult(kv.getKey(), "skipped"));
        }
        allTestResults.put(p.getFileName().toString(), new AllTestResults(resultList));
      }
    }
    File tempFile = File.createTempFile("sql-on-fhir-v2-test-result-", ".json");
    try (FileWriter writer = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
      Gson writerGson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
      writerGson.toJson(allTestResults, writer);
    }
  }

  @SuppressWarnings("NullAway.Init")
  private static class TestDef {
    String title;
    List<String> fhirVersion;
    List<JsonObject> resources;
    List<SingleTest> tests;
  }

  @SuppressWarnings("NullAway.Init")
  private static class SingleTest {
    String title;
    ViewDefinition view;
    List<JsonObject> expect;
    Boolean expectError;
  }

  // Suppressing UnusedVariable because this is just a data class used by runAllTests() to serialize
  // to file
  @SuppressWarnings({"NullAway.Init", "UnusedVariable"})
  private static class SingleTestResult {
    final String name;
    final Result result;

    // Suppressing UnusedVariable because this is just a data class used by runAllTests() to
    // serialize to file
    @SuppressWarnings({"NullAway.Init", "UnusedVariable"})
    private static class Result {
      final boolean passed;
      @Nullable final String reason;

      private Result(boolean passed, @Nullable String reason) {
        this.passed = passed;
        this.reason = reason;
      }
    }

    SingleTestResult(String name, boolean passed) {
      this.name = name;
      result = new Result(passed, null);
    }

    SingleTestResult(String name, String failureReason) {
      this.name = name;
      result = new Result(false, failureReason);
    }
  }

  // Suppressing UnusedVariable because this is just a data class used by runAllTests() to
  // serialize to file
  @SuppressWarnings("UnusedVariable")
  private static class AllTestResults {
    List<SingleTestResult> tests;

    AllTestResults(List<SingleTestResult> testResults) {
      this.tests = testResults;
    }
  }

  private static class ExpectedRows {
    private final List<Map<String, Object>> rows;

    private ExpectedRows(List<JsonObject> jsonArray) {
      rows = new ArrayList<>();
      for (JsonObject jsonObject : jsonArray) {
        Map<String, Object> row = new HashMap<>();
        for (String key : jsonObject.keySet()) {
          row.put(key, jsonObject.get(key));
        }
        rows.add(row);
      }
    }

    int getNumRows() {
      return rows.size();
    }

    private boolean hasRow(FlatRow row) {
      for (Map<String, Object> myRow : rows) {
        boolean matches = true;
        for (RowElement e : row.getElements()) {
          if (!myRow.containsKey(e.getName())) {
            matches = false;
            break;
          }
          Object myValue = myRow.get(e.getName());
          if (myValue instanceof JsonArray myList) {
            List<IBase> otherList = e.getValues();
            if (myList.size() != (otherList != null ? otherList.size() : 0)) {
              matches = false;
              break;
            }
            for (int i = 0; i < myList.size(); i++) {
              if (otherList == null || !typeSafeMatch(myList.get(i), otherList.get(i))) {
                matches = false;
                break;
              }
            }
            if (!matches) break;
          } else {
            if (myValue instanceof JsonNull) {
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

    private boolean typeSafeMatch(Object expected, @Nullable IBase actual) {
      if (actual == null) return false;
      if (expected.equals(actual)) return true;
      // TODO add other types as required by tests.
      if (actual instanceof IPrimitiveType<?>) {
        if (!(expected instanceof JsonPrimitive expectedPrimitive)) {
          return false;
        }
        if (actual instanceof IIdType actualIIdType) {
          return actualIIdType.getIdPart().equals(expectedPrimitive.getAsString());
        }
        if (actual instanceof IBaseDecimalDatatype actualIBaseDecimalDatatype) {
          BigDecimal expectedBigDecimal = null;
          if (expectedPrimitive.isNumber()) {
            expectedBigDecimal = expectedPrimitive.getAsBigDecimal();
          }
          return (expectedBigDecimal != null)
              && actualIBaseDecimalDatatype.getValue().compareTo(expectedBigDecimal) == 0;
        }
        String stringValue = ((IPrimitiveType<?>) actual).getValueAsString();
        return stringValue.equals(expectedPrimitive.getAsString());
      }
      return false;
    }
  }
}
