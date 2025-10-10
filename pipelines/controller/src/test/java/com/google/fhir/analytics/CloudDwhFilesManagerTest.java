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

import com.google.api.services.storage.model.StorageObject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = DataProperties.class)
@TestPropertySource("classpath:application-test.properties")
@EnableConfigurationProperties(value = DataProperties.class)
public class CloudDwhFilesManagerTest {

  @SuppressWarnings("NullAway.Init")
  @Mock
  private GcsUtil mockGcsUtil;

  private AutoCloseable closeable;

  @Autowired private DataProperties dataProperties;

  // Suppressing because of mockGcsUtil which is a mock object, initialization is via @Mock
  // annotation
  @SuppressWarnings("NullAway.Init")
  @BeforeEach
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcsUtil(mockGcsUtil);
    FileSystems.setDefaultPipelineOptions(gcsOptions);
  }

  @AfterEach
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
  public void testBaseDir() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
    String baseDir1 = dwhFilesManager.getBaseDir("gs://test-bucket/baseDir/prefix");
    assertThat(baseDir1, equalTo("gs://test-bucket/baseDir"));

    String baseDir2 = dwhFilesManager.getBaseDir("gs://test-bucket/baseDir/childDir/prefix");
    assertThat(baseDir2, equalTo("gs://test-bucket/baseDir/childDir"));

    String s3BaseDir = dwhFilesManager.getBaseDir("s3://test-bucket/baseDir/childDir/prefix");
    assertThat(s3BaseDir, equalTo("s3://test-bucket/baseDir/childDir"));
  }

  @Test
  public void testBaseDirForInvalidPath() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
          dwhFilesManager.getBaseDir("gs://test-bucket");
        });
  }

  @Test
  public void testBaseDirForInvalidPathS3() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
          dwhFilesManager.getBaseDir("s3://test-bucket");
        });
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

    String s3Prefix = dwhFilesManager.getPrefix("s3://test-bucket/baseDir/childDir/prefix");
    assertThat(s3Prefix, equalTo("prefix"));
  }

  @Test
  public void testPrefixForInvalidPathS3() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
          // S3 Path should end with a non-empty suffix string after the last occurrence of the
          // character '/'
          dwhFilesManager.getPrefix("s3://test-bucket/baseDir/");
        });
  }

  @Test
  public void testPrefixForInvalidPath() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          DwhFilesManager dwhFilesManager = new DwhFilesManager(dataProperties);
          // GCS Path should end with a non-empty suffix string after the last occurrence of the
          // character '/'
          dwhFilesManager.getPrefix("gs://test-bucket/baseDir/");
        });
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
