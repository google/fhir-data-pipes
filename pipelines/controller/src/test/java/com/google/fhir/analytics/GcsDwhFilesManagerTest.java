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
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataProperties.class)
@TestPropertySource("classpath:application-test.properties")
@EnableConfigurationProperties(value = DataProperties.class)
public class GcsDwhFilesManagerTest {

  @Mock private GcsUtil mockGcsUtil;
  private AutoCloseable closeable;

  @Autowired private DataProperties dataProperties;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcsUtil(mockGcsUtil);
    FileSystems.setDefaultPipelineOptions(gcsOptions);
  }

  @After
  public void closeService() throws Exception {
    closeable.close();
  }

  @Test
  public void testIsDwhComplete_True() throws IOException {
    String timestampStartFile = "gs://testbucket/testdirectory/timestamp_start.txt";
    List<StorageObjectOrIOException> startItems = new ArrayList<>();
    // Files within the directory
    startItems.add(
        StorageObjectOrIOException.create(
            createStorageObject(timestampStartFile, 1L /* fileSize */)));
    Mockito.when(mockGcsUtil.getObjects(List.of(GcsPath.fromUri(timestampStartFile))))
        .thenReturn(startItems);

    String timestampEndFile = "gs://testbucket/testdirectory/timestamp_end.txt";
    List<StorageObjectOrIOException> endItems = new ArrayList<>();
    // Files within the directory
    endItems.add(
        StorageObjectOrIOException.create(
            createStorageObject(timestampEndFile, 1L /* fileSize */)));
    Mockito.when(mockGcsUtil.getObjects(List.of(GcsPath.fromUri(timestampEndFile))))
        .thenReturn(endItems);

    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);

    ResourceId dwhRoot = FileSystems.matchNewResource("gs://testbucket/testdirectory", true);
    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(true));
  }

  @Test
  public void testIsDwhComplete_False() throws IOException {
    String timestampStartFile = "gs://testbucket/testdirectory/timestamp_start.txt";
    List<StorageObjectOrIOException> startItems = new ArrayList<>();
    // Files within the directory
    startItems.add(
        StorageObjectOrIOException.create(
            createStorageObject(timestampStartFile, 1L /* fileSize */)));
    Mockito.when(mockGcsUtil.getObjects(List.of(GcsPath.fromUri(timestampStartFile))))
        .thenReturn(startItems);

    String timestampEndFile = "gs://testbucket/testdirectory/timestamp_end.txt";
    List<StorageObjectOrIOException> items = new ArrayList<>();
    items.add(StorageObjectOrIOException.create(new FileNotFoundException()));
    Mockito.when(mockGcsUtil.getObjects(List.of(GcsPath.fromUri(timestampEndFile))))
        .thenReturn(items);

    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);

    ResourceId dwhRoot = FileSystems.matchNewResource("gs://testbucket/testdirectory", true);
    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(false));
  }

  @Test
  public void testGetAllChildDirectoriesOneLevelDeep() throws IOException {
    Objects modelObjects = new Objects();
    List<StorageObject> items = new ArrayList<>();
    // Files within the directory
    items.add(
        createStorageObject(
            "gs://testbucket/testdirectory/Patient/patient.parquet", 1L /* fileSize */));
    items.add(
        createStorageObject(
            "gs://testbucket/testdirectory/Observation/observation.parquet", 2L /* fileSize */));
    modelObjects.setItems(items);

    Mockito.when(
            mockGcsUtil.listObjects(
                Mockito.eq("testbucket"), Mockito.anyString(), Mockito.isNull()))
        .thenReturn(modelObjects);

    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
    Set<ResourceId> childDirectories =
        dwhFilesManager.getAllChildDirectories("gs://testbucket/testdirectory");

    assertThat(childDirectories.size(), equalTo(2));
    assertThat(
        childDirectories.contains(
            FileSystems.matchNewResource("gs://testbucket/testdirectory/Patient", true)),
        equalTo(true));
    assertThat(
        childDirectories.contains(
            FileSystems.matchNewResource("gs://testbucket/testdirectory/Observation", true)),
        equalTo(true));
  }

  @Test
  public void testBaseDir() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
    String baseDir1 = dwhFilesManager.getBaseDir("gs://test-bucket/baseDir/prefix");
    assertThat(baseDir1, equalTo("gs://test-bucket/baseDir"));

    String baseDir2 = dwhFilesManager.getBaseDir("gs://test-bucket/baseDir/childDir/prefix");
    assertThat(baseDir2, equalTo("gs://test-bucket/baseDir/childDir"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBaseDirForInvalidPath() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
    dwhFilesManager.getBaseDir("gs://test-bucket");
  }

  @Test
  public void testPrefix() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);

    String prefix1 = dwhFilesManager.getPrefix("gs://test-bucket/prefix");
    assertThat(prefix1, equalTo("prefix"));

    String prefix2 = dwhFilesManager.getPrefix("gs://test-bucket/baseDir/prefix");
    assertThat(prefix2, equalTo("prefix"));

    String prefix3 = dwhFilesManager.getPrefix("gs://test-bucket/baseDir/childDir/prefix");
    assertThat(prefix3, equalTo("prefix"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrefixForInvalidPath() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
    // GCS Path should end with a non-empty suffix string after the last occurrence of the
    // character '/'
    dwhFilesManager.getPrefix("gs://test-bucket/baseDir/");
  }

  private StorageObject createStorageObject(String gcsFilename, long fileSize) {
    GcsPath gcsPath = GcsPath.fromUri(gcsFilename);
    // Google APIs will use null for empty files.
    @Nullable BigInteger size = (fileSize == 0) ? null : BigInteger.valueOf(fileSize);
    return new StorageObject()
        .setBucket(gcsPath.getBucket())
        .setName(gcsPath.getObject())
        .setSize(size);
  }
}
