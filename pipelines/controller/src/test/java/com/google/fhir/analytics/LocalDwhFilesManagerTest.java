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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@TestPropertySource("classpath:application-test.properties")
@EnableConfigurationProperties(value = DataProperties.class)
@RunWith(SpringRunner.class)
@SpringBootTest(classes = DataProperties.class)
public class LocalDwhFilesManagerTest {

  @Autowired private DataProperties dataProperties;

  private DwhFilesManager dwhFilesManager;

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    dwhFilesManager = new DwhFilesManager(dataProperties);
    dwhFilesManager.init();
  }

  @Test
  public void testCheckPurgeScheduleAndTrigger() throws IOException {
    File rootDir = testFolder.newFolder("rootDir");
    dataProperties.setDwhRootPrefix(rootDir.getPath() + "/snapshot");
    dwhFilesManager.init();

    createCompleteDwhSnapshot("rootDir/snapshot1");
    createCompleteDwhSnapshot("rootDir/snapshot2");
    createCompleteDwhSnapshot("rootDir/snapshot3");
    createCompleteDwhSnapshot("rootDir/snapshot4");
    createCompleteDwhSnapshot("rootDir/snapshot5");

    dwhFilesManager.checkPurgeScheduleAndTrigger();
    // Check if the number of the snapshots remaining after the purge job is equal to the
    // configured retain number
    assertThat(rootDir.listFiles().length, equalTo(dataProperties.getNumOfDwhSnapshotsToRetain()));
  }

  @Test
  public void testIsDwhComplete_True() throws IOException {
    File root = testFolder.newFolder("DWH_SOURCE_TEST");
    Path startTimestampPath = Paths.get(root.getPath(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    Path endTimestampPath = Paths.get(root.getPath(), "timestamp_end.txt");
    createFile(endTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    ResourceId dwhRoot = FileSystems.matchNewResource(root.getPath(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(true));
  }

  @Test
  public void testIsDwhComplete_False() throws IOException {
    File root = testFolder.newFolder("DWH_SOURCE_TEST");
    Path startTimestampPath = Paths.get(root.getPath(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    ResourceId dwhRoot = FileSystems.matchNewResource(root.getPath(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(false));
  }

  @Test
  public void testGetAllChildDirectoriesOneLevelDeep() throws IOException {
    File rootDir = testFolder.newFolder("rootDir");
    Path childDir1 = Paths.get(rootDir.getPath(), "childDir1");
    Files.createDirectories(childDir1);
    Path fileAtChildDir1 = Path.of(childDir1.toString(), "file1.txt");
    createFile(fileAtChildDir1, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));
    Path childDir2 = Paths.get(rootDir.getPath(), "childDir2");
    Files.createDirectories(childDir2);
    Path fileAtChildDir2 = Path.of(childDir2.toString(), "file2.txt");
    createFile(fileAtChildDir2, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));

    Set<ResourceId> childDirectories = dwhFilesManager.getAllChildDirectories(rootDir.getPath());

    assertThat(childDirectories.size(), equalTo(2));
    assertThat(
        childDirectories.contains(FileSystems.matchNewResource(childDir1.toString(), true)),
        equalTo(true));
    assertThat(
        childDirectories.contains(FileSystems.matchNewResource(childDir2.toString(), true)),
        equalTo(true));
  }

  @Test
  public void testBaseDir() {
    String baseDir1 = dwhFilesManager.getBaseDir("/root/prefix");
    assertThat(baseDir1, equalTo("/root"));

    String baseDir2 = dwhFilesManager.getBaseDir("/root/child/prefix");
    assertThat(baseDir2, equalTo("/root/child"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBaseDirForInvalidPath() {
    dwhFilesManager.getBaseDir("/root");
  }

  @Test
  public void testPrefix() {
    String prefix1 = dwhFilesManager.getPrefix("/root/prefix");
    assertThat(prefix1, equalTo("prefix"));

    String prefix2 = dwhFilesManager.getPrefix("/root/child/prefix");
    assertThat(prefix2, equalTo("prefix"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrefixForInvalidPath() {
    dwhFilesManager.getPrefix("/prefix");
  }

  private void createCompleteDwhSnapshot(String rootPath) throws IOException {
    File rootDir = testFolder.newFolder(rootPath);
    Path childDir1 = Paths.get(rootDir.getPath(), "childDir1");
    Files.createDirectories(childDir1);
    Path fileAtChildDir1 = Path.of(childDir1.toString(), "file1.txt");
    createFile(fileAtChildDir1, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));
    Path childDir2 = Paths.get(rootDir.getPath(), "childDir2");
    Files.createDirectories(childDir2);
    Path fileAtChildDir2 = Path.of(childDir2.toString(), "file2.txt");
    createFile(fileAtChildDir2, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));

    Path startTimestampPath = Paths.get(rootDir.getPath(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    Path endTimestampPath = Paths.get(rootDir.getPath(), "timestamp_end.txt");
    createFile(endTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
  }

  private void createFile(Path path, byte[] bytes) throws IOException {
    Files.write(path, bytes);
  }
}
