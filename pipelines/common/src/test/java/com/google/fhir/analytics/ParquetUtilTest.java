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
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.io.Resources;
import com.google.fhir.analytics.view.ViewApplicationException;
import com.google.fhir.analytics.view.ViewDefinitionException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

// TODO add testes for DSTU3 resources too.

@ExtendWith(MockitoExtension.class)
public class ParquetUtilTest {

  private static final String PARQUET_ROOT = "parquet_root";

  // This does not have to be a sub-dir of PARQUET_ROOT; ParquetUtil should be able to handle both.
  private static final String PARQUET_VIEW_ROOT = "parquet_view_root";

  private String observationBundle;

  private ParquetUtil parquetUtil;

  private AvroConversionUtil avroConversionUtil;

  private Path rootPath;

  private Path viewRootPath;

  @TempDir File temporaryFolder;

  // Initialization handled by JUnit @BeforeEach annotation
  @SuppressWarnings("NullAway.Init")
  @BeforeEach
  public void setup() throws IOException, ProfileException {
    File rootFolder = new File(temporaryFolder, PARQUET_ROOT);
    rootPath = Paths.get(rootFolder.getPath());
    Files.createDirectories(rootPath);
    File viewFolder = new File(temporaryFolder, PARQUET_VIEW_ROOT);
    viewRootPath = Paths.get(viewFolder.getPath());
    Files.createDirectories(viewRootPath);
    String viewDefPath = Resources.getResource("parquet-util-view-test").getFile();
    AvroConversionUtil.initializeAvroConverters();

    observationBundle =
        Resources.toString(
            Resources.getResource("observation_bundle.json"), StandardCharsets.UTF_8);
    AvroConversionUtil.deRegisterMappingsFor(FhirVersionEnum.R4);
    avroConversionUtil = AvroConversionUtil.getInstance(FhirVersionEnum.R4, "", 1);
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4,
            "",
            rootPath.toString(),
            // TODO add unit-tests for empty rootPath and non-empty input root.
            "",
            viewDefPath,
            viewRootPath.toString(),
            0,
            0,
            "TEST_",
            1,
            false);
  }

  @Test
  public void bestOutputFile_NoDir() throws IOException {
    ResourceId bestFile = parquetUtil.getUniqueOutputFilePath("Patient");
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());

    String path = rootPath.toString() + fileSeparator + "Patient" + fileSeparator;
    path = path.replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("\\\\"));
    String patternToBeMatched =
        path
            + "TEST_Patient_output-parquet-th-[\\p{Digit}]+-ts-[\\p{Digit}]+-r-[\\p{Digit}]+.parquet";
    assertThat(bestFile.toString(), matchesPattern(patternToBeMatched));
  }

  /** Tests the output naming convention for Materialized ViewDefinition Parquet Files */
  @Test
  public void bestOutputFile_NoDirView() throws IOException {
    ResourceId bestFile = parquetUtil.getUniqueOutputFilePathView("patient_flat");
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());

    String path = viewRootPath.toString() + fileSeparator + "patient_flat" + fileSeparator;
    path = path.replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("\\\\"));
    String patternToBeMatched =
        path
            + "TEST_patient_flat_output-parquet-th-[\\p{Digit}]+-ts-[\\p{Digit}]+-r-[\\p{Digit}]+.parquet";
    assertThat(bestFile.toString(), matchesPattern(patternToBeMatched));
  }

  @Test
  public void bestOutputFile_NoFiles() throws IOException {
    Path patientPath = rootPath.resolve("Patient");
    Files.createDirectory(patientPath);
    ResourceId bestFile = parquetUtil.getUniqueOutputFilePath("Patient");
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());

    String path = rootPath.toString() + fileSeparator + "Patient" + fileSeparator;
    path = path.replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("\\\\"));
    String patternToBeMatched =
        path
            + "TEST_Patient_output-parquet-th-[\\p{Digit}]+-ts-[\\p{Digit}]+-r-[\\p{Digit}]+.parquet";
    assertThat(bestFile.toString(), matchesPattern(patternToBeMatched));
  }

  @Test
  public void createSingleOutput() throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4, "", rootPath.toString(), "", "", null, 0, 0, "", 1, false);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      parquetUtil.write(entry.getResource());
    }
    parquetUtil.flushAllWritersAndStopTimer();
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());
    try (Stream<Path> stream =
        Files.list(rootPath.resolve("Observation"))
            .filter(
                f ->
                    f.toString()
                        .startsWith(
                            rootPath.toString()
                                + fileSeparator
                                + "Observation"
                                + fileSeparator
                                + "Observation_output-"))) {
      List<Path> files = stream.toList();
      assertThat(files.size(), equalTo(1));
    }
  }

  @Test
  public void createMultipleOutputByTime()
      throws IOException, InterruptedException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4, "", rootPath.toString(), "", "", null, 1, 0, "", 1, false);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      parquetUtil.write(entry.getResource());
      TimeUnit.SECONDS.sleep(2); // A better way to test this is to inject a mocked `Timer`.
    }
    parquetUtil.flushAllWritersAndStopTimer();
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());
    try (Stream<Path> fileStream = Files.list(rootPath.resolve("Observation"))) {
      long filesCount =
          fileStream
              .filter(
                  f ->
                      f.toString()
                          .startsWith(
                              rootPath.toString()
                                  + fileSeparator
                                  + "Observation"
                                  + fileSeparator
                                  + "Observation_output-"))
              .count();
      assertThat(filesCount, equalTo(6L));
    }
  }

  /**
   * The point of this test is to reduce the row-group size and show we still get a single file when
   * there are multiple groups.
   */
  @Test
  public void createSingleOutputWithRowGroupSize()
      throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    // This came from debugging `InternalParquetRecordWriter`!
    final int approximateMemSizeOfOneObservarion = 458;
    // We could also set a very small rowGroupSize to force multiple groups but because
    // each group has at least 100 records (regardless of its size), then setting a very
    // small number will trigger this warning which creates thousands of log lines:
    // https://github.com/apache/parquet-java/blob/fb6f0be0323f5f52715b54b8c6602763d8d0128d/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/InternalParquetRecordWriter.java#L203
    final int rowGroupSize = approximateMemSizeOfOneObservarion * 90;
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4,
            "",
            rootPath.toString(),
            "",
            "",
            null,
            0,
            rowGroupSize,
            "",
            1,
            false);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    // There are 6 resources in the bundle, so we write 17*6 (>100) resources, such that the page
    // group size check is triggered but still it is expected to generate one file only.
    for (int i = 0; i < 17; i++) {
      for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
        parquetUtil.write(entry.getResource());
      }
    }
    parquetUtil.flushAllWritersAndStopTimer();
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());
    List<Path> files;
    try (var fileStream = Files.list(rootPath.resolve("Observation"))) {
      files =
          fileStream
              .filter(
                  f ->
                      f.toString()
                          .startsWith(
                              rootPath.toString()
                                  + fileSeparator
                                  + "Observation"
                                  + fileSeparator
                                  + "Observation_output-"))
              .toList();
      // We expect only one file to be created.
      assertThat(files.size(), equalTo(1));
    }
    HadoopInputFile file =
        HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(files.get(0).toUri()), new Configuration());
    ParquetFileReader reader = ParquetFileReader.open(file);
    // Verify that two row groups have been created:
    assertThat(reader.getRowGroups().size(), equalTo(2));
  }

  /** Tests the ParquetUtil write method for Materialized ViewDefinitions */
  @Test
  public void createOutputViewToParquet()
      throws IOException, ProfileException, ViewApplicationException, ViewDefinitionException {
    String fileSeparator = DwhFiles.getFileSeparatorForDwhFiles(rootPath.toString());
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();

    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      parquetUtil.write(entry.getResource());
    }
    parquetUtil.flushAllWritersAndStopTimer();

    try (Stream<Path> files = Files.list(viewRootPath.resolve("observation_flat"))) {

      long filesCount =
          files
              .filter(
                  f ->
                      f.toString()
                          .startsWith(
                              viewRootPath.toString()
                                  + fileSeparator
                                  + "observation_flat"
                                  + fileSeparator
                                  + "TEST_observation_flat_output-"))
              .count();
      assertThat(filesCount, equalTo(1L));
    }
  }

  /**
   * This is the test to demonstrate the BigDecimal conversion bug. See:
   * https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/156
   */
  @Test
  public void writeObservationWithBigDecimalValue()
      throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4, "", rootPath.toString(), "", "", null, 0, 0, "", 1, false);
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    parquetUtil.write(observation);
  }

  /** This is similar to the above test but has more `decimal` examples with different scales. */
  @Test
  public void writeObservationBundleWithDecimalConversionIssue()
      throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4, "", rootPath.toString(), "", "", null, 0, 0, "", 1, false);
    String observationBundleStr =
        Resources.toString(
            Resources.getResource("observation_decimal_bundle.json"), StandardCharsets.UTF_8);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundleStr);
    parquetUtil.writeRecords(bundle, null);
  }

  /** This test check if the same resource with multiple profiles get written to files. */
  @Test
  public void writeObservationBundleWithMultipleProfiles()
      throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4, "", rootPath.toString(), "", "", null, 0, 0, "", 1, false);

    String patientStr =
        Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientStr);
    parquetUtil.writeRecords(bundle, null);

    String patientUsCoreStr =
        Resources.toString(
            Resources.getResource("patient_bundle_us_core_profile.json"), StandardCharsets.UTF_8);
    IParser parser2 = avroConversionUtil.getFhirContext().newJsonParser();
    Bundle bundle2 = parser2.parseResource(Bundle.class, patientUsCoreStr);
    parquetUtil.writeRecords(bundle2, null);

    parquetUtil.flushAllWritersAndStopTimer();
  }

  @Test
  public void writeQuestionnaireResponse()
      throws IOException, ProfileException, ViewApplicationException {
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    AvroConversionUtil.deRegisterMappingsFor(FhirVersionEnum.R4);
    avroConversionUtil =
        AvroConversionUtil.getInstance(FhirVersionEnum.R4, "classpath:/r4-us-core-definitions", 1);
    parquetUtil =
        new ParquetUtil(
            FhirVersionEnum.R4,
            "classpath:/r4-us-core-definitions",
            rootPath.toString(),
            "",
            "",
            null,
            0,
            0,
            "",
            1,
            false);

    String questionnaireResponseStr =
        Resources.toString(
            Resources.getResource("questionnaire_response.json"), StandardCharsets.UTF_8);
    IParser parser = avroConversionUtil.getFhirContext().newJsonParser();
    QuestionnaireResponse questionnaireResponse =
        parser.parseResource(QuestionnaireResponse.class, questionnaireResponseStr);
    parquetUtil.write(questionnaireResponse);

    parquetUtil.flushAllWritersAndStopTimer();
    AvroConversionUtil.deRegisterMappingsFor(FhirVersionEnum.R4);
  }
}
