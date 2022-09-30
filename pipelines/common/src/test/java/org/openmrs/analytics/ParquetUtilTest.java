/*
 * Copyright 2020-2022 Google LLC
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
package org.openmrs.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO add testes for DSTU3 resources too.

@RunWith(MockitoJUnitRunner.class)
public class ParquetUtilTest {

  private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);

  private static final String PARQUET_ROOT = "/parquet_root";

  private String patientBundle;

  private String observationBundle;

  private ParquetUtil parquetUtil;

  private FhirContext fhirContext;

  private FileSystem fileSystem;

  private Path rootPath;

  @Before
  public void setup() throws IOException {
    fileSystem = Jimfs.newFileSystem(Configuration.unix());
    rootPath = fileSystem.getPath(PARQUET_ROOT);
    Files.createDirectories(rootPath);
    ParquetUtil.initializeAvroConverters();
    patientBundle =
        Resources.toString(Resources.getResource("patient_bundle.json"), StandardCharsets.UTF_8);
    observationBundle =
        Resources.toString(
            Resources.getResource("observation_bundle.json"), StandardCharsets.UTF_8);
    this.fhirContext = FhirContext.forR4();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, PARQUET_ROOT, 0, 0, "TEST_", fileSystem);
  }

  @Test
  public void getResourceSchema_Patient() {
    Schema schema = parquetUtil.getResourceSchema("Patient");
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("name").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Observation() {
    Schema schema = parquetUtil.getResourceSchema("Observation");
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("basedOn").toString(), notNullValue());
    assertThat(schema.getField("subject").toString(), notNullValue());
    assertThat(schema.getField("code").toString(), notNullValue());
  }

  @Test
  public void getResourceSchema_Encounter() {
    Schema schema = parquetUtil.getResourceSchema("Encounter");
    assertThat(schema.getField("id").toString(), notNullValue());
    assertThat(schema.getField("identifier").toString(), notNullValue());
    assertThat(schema.getField("status").toString(), notNullValue());
    assertThat(schema.getField("class").toString(), notNullValue());
  }

  @Test
  public void generateRecords_BundleOfPatients() {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    List<GenericRecord> recordList = parquetUtil.generateRecords(bundle);
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
    List<GenericRecord> recordList = parquetUtil.generateRecords(bundle);
    assertThat(recordList.size(), equalTo(6));
  }

  @Test
  public void generateRecordForPatient() {
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, patientBundle);
    GenericRecord record = parquetUtil.convertToAvro(bundle.getEntry().get(0).getResource());
    Collection<Object> addressList = (Collection<Object>) record.get("address");
    // TODO We need to fix this again; the root cause is the change in `id` type to System.String.
    // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/55
    // assertThat(record.get("id"), equalTo("471be3bc-08c7-4d78-a4ab-1b3d044dae67"));
    assertThat(addressList.size(), equalTo(1));
    Record address = (Record) addressList.iterator().next();
    assertThat((String) address.get("city"), equalTo("Waterloo"));
  }

  @Test
  public void bestOutputFile_NoDir() throws IOException {
    org.apache.hadoop.fs.Path bestFile = parquetUtil.uniqueOutputFile("Patient");
    assertThat(
        bestFile.toString(),
        matchesPattern(
            "/parquet_root/Patient/TEST_Patient_output-parquet-th-[\\p{Digit}]+-ts-[\\p{Digit}]+-r-[\\p{Digit}]+"));
  }

  @Test
  public void bestOutputFile_NoFiles() throws IOException {
    Path patientPath = rootPath.resolve("Patient");
    Files.createDirectory(patientPath);
    org.apache.hadoop.fs.Path bestFile = parquetUtil.uniqueOutputFile("Patient");
    assertThat(
        bestFile.toString(),
        matchesPattern(
            "/parquet_root/Patient/TEST_Patient_output-parquet-th-[\\p{Digit}]+-ts-[\\p{Digit}]+-r-[\\p{Digit}]+"));
  }

  private void initilizeLocalFileSystem() throws IOException {
    // TODO: if we could convince ParquetWriter to use an input FileSystem we could use `Jimfs`.
    fileSystem = FileSystems.getDefault();
    rootPath = Files.createTempDirectory("PARQUET_TEST");
    log.info("Temporary directory is " + rootPath);
  }

  @Test
  public void createSingleOutput() throws IOException {
    initilizeLocalFileSystem();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, rootPath.toString(), 0, 0, "", fileSystem);
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      parquetUtil.write(entry.getResource());
    }
    parquetUtil.closeAllWriters();
    Stream<Path> files =
        Files.list(rootPath.resolve("Observation"))
            .filter(
                f ->
                    f.toString()
                        .startsWith(rootPath.toString() + "/Observation/Observation_output-"));
    assertThat(files.count(), equalTo(1L));
  }

  @Test
  public void createMultipleOutputByTime() throws IOException, InterruptedException {
    initilizeLocalFileSystem();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, rootPath.toString(), 1, 0, "", fileSystem);
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
      parquetUtil.write(entry.getResource());
      TimeUnit.SECONDS.sleep(2); // A better way to test this is to inject a mocked `Timer`.
    }
    parquetUtil.closeAllWriters();
    Stream<Path> files =
        Files.list(rootPath.resolve("Observation"))
            .filter(
                f ->
                    f.toString()
                        .startsWith(rootPath.toString() + "/Observation/Observation_output-"));
    assertThat(files.count(), equalTo(7L));
  }

  @Test
  public void createSingleOutputWithRowGroupSize() throws IOException {
    initilizeLocalFileSystem();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, rootPath.toString(), 0, 1, "", fileSystem);
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundle);
    // There are 7 resources in the bundle so we write 15*7 (>100) resources, such that the page
    // group size check is triggered but still it is expected to generate one file only.
    for (int i = 0; i < 15; i++) {
      for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
        parquetUtil.write(entry.getResource());
      }
    }
    parquetUtil.closeAllWriters();
    Stream<Path> files =
        Files.list(rootPath.resolve("Observation"))
            .filter(
                f ->
                    f.toString()
                        .startsWith(rootPath.toString() + "/Observation/Observation_output-"));
    assertThat(files.count(), equalTo(1L));
  }

  @Test
  public void convertObservationWithBigDecimalValue() throws IOException {
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    GenericRecord record = parquetUtil.convertToAvro(observation);
    GenericData.Record valueRecord = (GenericData.Record) record.get("value");
    BigDecimal value = (BigDecimal) ((GenericData.Record) valueRecord.get("quantity")).get("value");
    assertThat(value.doubleValue(), closeTo(1.8287, 0.001));
  }

  /**
   * This is the test to demonstrate the BigDecimal conversion bug. See:
   * https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/156
   */
  @Test
  public void writeObservationWithBigDecimalValue() throws IOException {
    initilizeLocalFileSystem();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, rootPath.toString(), 0, 0, "", fileSystem);
    String observationStr =
        Resources.toString(
            Resources.getResource("observation_decimal.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Observation observation = parser.parseResource(Observation.class, observationStr);
    parquetUtil.write(observation);
  }

  /** This is similar to the above test but has more `decimal` examples with different scales. */
  @Test
  public void writeObservationBundleWithDecimalConversionIssue() throws IOException {
    initilizeLocalFileSystem();
    parquetUtil = new ParquetUtil(FhirVersionEnum.R4, rootPath.toString(), 0, 0, "", fileSystem);
    String observationBundleStr =
        Resources.toString(
            Resources.getResource("observation_decimal_bundle.json"), StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    Bundle bundle = parser.parseResource(Bundle.class, observationBundleStr);
    parquetUtil.writeRecords(bundle, null);
  }
}
