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
package org.openmrs.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Test;

public class LocalDwhFilesManagerTest {

  @Test
  public void testIsDWHComplete_True() throws IOException {
    Path root = Files.createTempDirectory("DWH_SOURCE_TEST");
    Path startTimestampPath = Paths.get(root.toString(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    Path endTimestampPath = Paths.get(root.toString(), "timestamp_end.txt");
    createFile(endTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    ResourceId dwhRoot = FileSystems.matchNewResource(root.toString(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(true));

    Files.delete(endTimestampPath);
    Files.delete(startTimestampPath);
    Files.delete(root);
  }

  @Test
  public void testIsDWHComplete_False() throws IOException {
    Path root = Files.createTempDirectory("DWH_SOURCE_TEST");
    Path startTimestampPath = Paths.get(root.toString(), "timestamp_start.txt");
    createFile(startTimestampPath, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    ResourceId dwhRoot = FileSystems.matchNewResource(root.toString(), true);

    assertThat(dwhFilesManager.isDwhComplete(dwhRoot), equalTo(false));

    Files.delete(startTimestampPath);
    Files.delete(root);
  }

  @Test
  public void testGetAllChildDirectoriesOneLevelDeep() throws IOException {
    Path rootDir = Files.createTempDirectory("rootDir");
    Path childDir1 = Paths.get(rootDir.toString(), "childDir1");
    Files.createDirectories(childDir1);
    Path fileAtChildDir1 = Path.of(childDir1.toString(), "file1.txt");
    createFile(fileAtChildDir1, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));
    Path childDir2 = Paths.get(rootDir.toString(), "childDir2");
    Files.createDirectories(childDir2);
    Path fileAtChildDir2 = Path.of(childDir2.toString(), "file2.txt");
    createFile(fileAtChildDir2, "SAMPLE TEXT".getBytes(StandardCharsets.UTF_8));
    DwhFilesManager dwhFilesManager = new DwhFilesManager();

    Set<ResourceId> childDirectories = dwhFilesManager.getAllChildDirectories(rootDir.toString());

    assertThat(childDirectories.size(), equalTo(2));
    assertThat(
        childDirectories.contains(FileSystems.matchNewResource(childDir1.toString(), true)),
        equalTo(true));
    assertThat(
        childDirectories.contains(
            FileSystems.matchNewResource(childDir2.toString(), true)),
        equalTo(true));

    Files.delete(fileAtChildDir1);
    Files.delete(childDir1);
    Files.delete(fileAtChildDir2);
    Files.delete(childDir2);
    Files.delete(rootDir);
  }

  @Test
  public void testBaseDir() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    String baseDir1 = dwhFilesManager.getBaseDir("/root/prefix");
    assertThat(baseDir1, equalTo("/root"));

    String baseDir2 = dwhFilesManager.getBaseDir("/root/child/prefix");
    assertThat(baseDir2, equalTo("/root/child"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBaseDirForInvalidPath() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    dwhFilesManager.getBaseDir("/root");
  }

  @Test
  public void testPrefix() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    String prefix1 = dwhFilesManager.getPrefix("/root/prefix");
    assertThat(prefix1, equalTo("prefix"));

    String prefix2 = dwhFilesManager.getPrefix("/root/child/prefix");
    assertThat(prefix2, equalTo("prefix"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrefixForInvalidPath() {
    DwhFilesManager dwhFilesManager = new DwhFilesManager();
    dwhFilesManager.getPrefix("/prefix/");
  }

  private void createFile(Path path, byte[] bytes) throws IOException {
    Files.write(path, bytes);
  }
}
