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
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@TestPropertySource("classpath:application-test.properties")
@EnableConfigurationProperties(value = DataProperties.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = DataProperties.class)
public class LocalDwhFilesManagerTest {

  @Autowired private DataProperties dataProperties;

  private DwhFilesManager dwhFilesManager;

  @TempDir public Path testPath;

  @BeforeEach
  public void setUp() {
    dwhFilesManager = new DwhFilesManager(dataProperties);
    dwhFilesManager.init();
  }

  @Test
  public void testCheckPurgeScheduleAndTrigger() throws IOException {
    Path rootDir = testPath.resolve("rootDir");
    dataProperties.setDwhRootPrefix(rootDir + File.separator + "snapshot");
    dwhFilesManager.init();

    createCompleteDwhSnapshot("rootDir" + File.separator + "snapshot1");
    createCompleteDwhSnapshot("rootDir" + File.separator + "snapshot2");
    createCompleteDwhSnapshot("rootDir" + File.separator + "snapshot3");
    createCompleteDwhSnapshot("rootDir" + File.separator + "snapshot4");
    createCompleteDwhSnapshot("rootDir" + File.separator + "snapshot5");

    dwhFilesManager.checkPurgeScheduleAndTrigger();
    // Check if the number of the snapshots remaining after the purge job is equal to the
    // configured retain number
    assertThat(
        Files.list(rootDir).collect(Collectors.toList()).size(),
        equalTo(dataProperties.getNumOfDwhSnapshotsToRetain()));
  }

  @Test
  public void testIsDwhComplete_True() throws IOException {
    Path root = testPath.resolve("DWH_SOURCE_TEST");
    Path startTimestampPath = root.resolve("timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    Path endTimestampPath = root.resolve("timestamp_end.txt");
    createFile(endTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    ResourceId dwhRoot = FileSystems.matchNewResource(root.toString(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(true));
  }

  @Test
  public void testIsDwhComplete_False() throws IOException {
    Path root = testPath.resolve("DWH_SOURCE_TEST");
    Path startTimestampPath = Paths.get(root.toString(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    ResourceId dwhRoot = FileSystems.matchNewResource(root.toString(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(false));
  }

  @Test
  public void testBaseDirForNonWindows() {
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    String baseDir1 = dwhFilesManager.getBaseDir("/root/prefix");
    assertThat(baseDir1, equalTo("/root"));

    String baseDir2 = dwhFilesManager.getBaseDir("/root/child/prefix");
    assertThat(baseDir2, equalTo("/root/child"));

    String baseDir3 = dwhFilesManager.getBaseDir("root/prefix");
    assertThat(baseDir3, equalTo("root"));

    String baseDir4 = dwhFilesManager.getBaseDir("root/child/prefix");
    assertThat(baseDir4, equalTo("root/child"));
  }

  @Test
  public void testBaseDirForWindows() {
    assumeTrue(SystemUtils.IS_OS_WINDOWS);
    String baseDir1 = dwhFilesManager.getBaseDir("C:\\prefix");
    assertThat(baseDir1, equalTo("C\\"));

    String baseDir2 = dwhFilesManager.getBaseDir("C:\\root\\prefix");
    assertThat(baseDir2, equalTo("C:\\root"));

    String baseDir3 = dwhFilesManager.getBaseDir("C:\\root\\child\\prefix");
    assertThat(baseDir3, equalTo("C:\\root\\child"));

    String baseDir4 = dwhFilesManager.getBaseDir("nonRoot\\prefix");
    assertThat(baseDir4, equalTo("nonRoot"));

    String baseDir5 = dwhFilesManager.getBaseDir("nonRoot\\child\\prefix");
    assertThat(baseDir5, equalTo("nonRoot\\child"));
  }

  @Test
  public void testBaseDirForInvalidPathNonWindows() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          assumeFalse(SystemUtils.IS_OS_WINDOWS);
          dwhFilesManager.getBaseDir("/root");
        });
  }

  @Test
  public void testBaseDirForInvalidPath2() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          dwhFilesManager.getBaseDir("root");
        });
  }

  @Test
  public void testPrefixForNonWindows() {
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    String prefix1 = dwhFilesManager.getPrefix("/root/prefix");
    assertThat(prefix1, equalTo("prefix"));

    String prefix2 = dwhFilesManager.getPrefix("/root/child/prefix");
    assertThat(prefix2, equalTo("prefix"));

    String prefix3 = dwhFilesManager.getPrefix("nonRoot/prefix");
    assertThat(prefix3, equalTo("prefix"));

    String prefix4 = dwhFilesManager.getPrefix("nonRoot/child/prefix");
    assertThat(prefix4, equalTo("prefix"));
  }

  @Test
  public void testPrefixForWindows() {
    assumeTrue(SystemUtils.IS_OS_WINDOWS);
    String prefix1 = dwhFilesManager.getBaseDir("C:\\prefix");
    assertThat(prefix1, equalTo("prefix"));

    String prefix2 = dwhFilesManager.getPrefix("C:\\root\\prefix");
    assertThat(prefix2, equalTo("prefix"));

    String prefix3 = dwhFilesManager.getPrefix("C:\\root\\child\\prefix");
    assertThat(prefix3, equalTo("prefix"));

    String prefix4 = dwhFilesManager.getPrefix("nonRoot\\prefix");
    assertThat(prefix4, equalTo("prefix"));

    String prefix5 = dwhFilesManager.getPrefix("nonRoot\\child\\prefix");
    assertThat(prefix5, equalTo("prefix"));
  }

  @Test
  public void testPrefixForInvalidPathNonWindows() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          assumeFalse(SystemUtils.IS_OS_WINDOWS);
          dwhFilesManager.getPrefix("/prefix");
        });
  }

  @Test
  public void testPrefixForInvalidPath2() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          dwhFilesManager.getPrefix("prefix");
        });
  }

  private void createCompleteDwhSnapshot(String rootPath) throws IOException {
    Path rootDir = testPath.resolve(rootPath);
    Path childDir1 = Paths.get(rootDir.toString(), "childDir1");
    Files.createDirectories(childDir1);
    Path fileAtChildDir1 = Path.of(childDir1.toString(), "file1.txt");
    createFile(fileAtChildDir1, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));
    Path childDir2 = Paths.get(rootDir.toString(), "childDir2");
    Files.createDirectories(childDir2);
    Path fileAtChildDir2 = Path.of(childDir2.toString(), "file2.txt");
    createFile(fileAtChildDir2, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));

    Path startTimestampPath = Paths.get(rootDir.toString(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    Path endTimestampPath = Paths.get(rootDir.toString(), "timestamp_end.txt");
    createFile(endTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
  }

  private void createFile(Path path, byte[] bytes) throws IOException {
    FileUtils.createParentDirectories(path.toFile());
    Files.write(path, bytes);
  }
}
