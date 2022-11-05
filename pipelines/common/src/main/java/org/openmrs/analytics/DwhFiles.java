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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwhFiles {

  private static final Logger log = LoggerFactory.getLogger(DwhFiles.class);

  private final String dwhRoot;

  private final FileSystem fileSystem;

  DwhFiles(String dwhRoot) {
    this(dwhRoot, FileSystems.getDefault());
  }

  @VisibleForTesting
  DwhFiles(String dwhRoot, FileSystem fileSystem) {
    Preconditions.checkNotNull(dwhRoot);
    this.dwhRoot = dwhRoot;
    this.fileSystem = fileSystem;
  }

  public String getParquetPath() {
    return dwhRoot;
  }

  /**
   * This returns the path to the directory which contains resources of a specific type. It will
   * also make sure the directory exists.
   *
   * @param resourceType the type of the FHIR resources
   * @return The path to the resource type dir.
   * @throws IOException if directory creation fails (won't fail if dir already exists).
   */
  public Path createResourcePath(String resourceType) throws IOException {
    // TODO Check this code on GCS and make it file-system agnostic:
    //   https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/390
    Path path = getResourcePath(resourceType);
    Files.createDirectories(path);
    return path;
  }

  /** Similar to {@code createResourcePath} but does not create the directory */
  public Path getResourcePath(String resourceType) {
    return fileSystem.getPath(getParquetPath(), resourceType);
  }

  public Set<String> findResourceTypes() throws IOException {
    // TODO: Switch to using Beam's FileSystems API or make sure this works on all platforms.
    //   Currently Beam's API fails to find directories under `dwhPath` with "*" wildcard and
    //   even with ResourceId.resolve; why? Sample pseudo-code:
    //   MatchResult matchResult = FileSystems.match(dwhRoot); // with "/*" it does not work either!
    //   ResourceId resourceId = matchResult -> metadata -> resourceId;
    //   resourceId.resolve("*", StandardResolveOptions.RESOLVE_DIRECTORY)
    //   https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/390
    Path dwhPath = fileSystem.getPath(dwhRoot);
    Set<String> typeSet =
        Files.list(dwhPath).map(p -> dwhPath.relativize(p).toString()).collect(Collectors.toSet());
    log.info("Resource types under {} are {}", dwhRoot, typeSet);
    return typeSet;
  }

  public void copyResourceType(String resourceType, String destDwh) throws IOException {
    Path path = fileSystem.getPath(getParquetPath(), resourceType);
    Path destPath = fileSystem.getPath(destDwh).resolve(resourceType);
    log.info("Copying {} to {}", path, destPath);
    FileUtils.copyDirectory(path.toFile(), destPath.toFile());
  }
}
