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

import ca.uhn.fhir.context.FhirContext;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DwhFilesTest {

  private FileSystem fileSystem;

  private DwhFiles instance;

  private Path root;

  @Before
  public void setup() throws IOException {
    // We cannot use Jimfs since it does not support Path.toFile().
    fileSystem = FileSystems.getDefault();
    root = Files.createTempDirectory("DWH_FILES_TEST");
    Path patientRoot = fileSystem.getPath(root.toString(), "Patient");
    Files.createDirectory(patientRoot);
    Path obsRoot = fileSystem.getPath(root.toString(), "Observation");
    Files.createDirectory(obsRoot);
    instance = new DwhFiles(root.toString(), fileSystem, FhirContext.forR4Cached());
  }

  @Test
  public void getResourcePathTest() {
    assertThat(
        instance.getResourcePath("Patient").toString(), equalTo(root.toString() + "/Patient"));
  }

  @Test
  public void findResourceTypesTest() throws IOException {
    Set<String> resourceTypes = instance.findResourceTypes();
    assertThat("Could not find Patient", resourceTypes.contains("Patient"));
    assertThat("Could not find Observation", resourceTypes.contains("Observation"));
    assertThat(resourceTypes.size(), equalTo(2));
  }

  @Test
  public void copyResourceTypeTest() throws IOException {
    Path destPath = Files.createTempDirectory("DWH_DEST_TEST");
    instance.copyResourcesToDwh("Patient", DwhFiles.forRoot(destPath.toString()));
    List<Path> destFiles = Files.list(destPath).collect(Collectors.toList());
    assertThat(destFiles.size(), equalTo(1));
    assertThat(destFiles.get(0).toString(), equalTo(destPath.resolve("Patient").toString()));
  }
}
