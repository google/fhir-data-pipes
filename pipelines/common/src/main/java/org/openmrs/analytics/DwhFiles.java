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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface for working with the data-warehouse files. This is where all file-structure logic
 * should be implemented. This should support different file-systems, including distributed ones.
 */
public class DwhFiles {

  private static final Logger log = LoggerFactory.getLogger(DwhFiles.class);

  private static final String TIMESTAMP_FILE = "timestamp.txt";

  private static final String INCREMENTAL_DIR = "incremental_run";

  private final String dwhRoot;

  private final FileSystem fileSystem;

  private final FhirContext fhirContext;

  private DwhFiles(String dwhRoot) {
    this(dwhRoot, FileSystems.getDefault(), FhirContext.forR4Cached());
  }

  static DwhFiles forRoot(String dwhRoot) {
    return new DwhFiles(dwhRoot);
  }

  DwhFiles(String dwhRoot, FileSystem fileSystem, FhirContext fhirContext) {
    Preconditions.checkNotNull(dwhRoot);
    this.dwhRoot = dwhRoot;
    this.fileSystem = fileSystem;
    this.fhirContext = fhirContext;
  }

  public String getRoot() {
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
    return fileSystem.getPath(getRoot(), resourceType);
  }

  /**
   * This returns the default incremental run path; each incremental run is relative to a full path,
   * hence we put this directory under the full-run root.
   *
   * @return the default incremental run path
   */
  public Path getIncrementalRunPath() {
    return fileSystem.getPath(getRoot(), INCREMENTAL_DIR);
  }

  /** This is used when we want to keep a backup of the old incremental run output. */
  public Path getIncrementalRunPathWithTimestamp() {
    return fileSystem.getPath(
        getRoot(), String.format("%s_old_%d", INCREMENTAL_DIR, System.currentTimeMillis()));
  }

  /**
   * Similar to {@link #getIncrementalRunPath} but also checks if that directory exists and if so,
   * moves it to {@link #getIncrementalRunPathWithTimestamp()}.
   *
   * @return same as {@link #getIncrementalRunPath()}
   * @throws IOException if the directory move fails
   */
  public Path newIncrementalRunPath() throws IOException {
    Path incPath = getIncrementalRunPath();
    if (hasIncrementalDir()) {
      Path movePath = getIncrementalRunPathWithTimestamp();
      log.info("Moving the old {} directory to {}", INCREMENTAL_DIR, movePath);
      Files.move(incPath, movePath);
    }
    return incPath;
  }

  /** @return true iff there is already an incremental run subdirectory in this DWH. */
  public boolean hasIncrementalDir() {
    return Files.exists(getIncrementalRunPath());
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
    Set<String> fileSet =
        Files.list(dwhPath).map(p -> dwhPath.relativize(p).toString()).collect(Collectors.toSet());
    Set<String> typeSet = Sets.newHashSet();
    for (String file : fileSet) {
      try {
        fhirContext.getResourceType(file);
        typeSet.add(file);
      } catch (DataFormatException e) {
        log.info(
            " HERE DEBUG ***************** Ignoring file {} which is not a FHIR resource.", file);
      }
    }
    log.info("Resource types under {} are {}", dwhRoot, typeSet);
    return typeSet;
  }

  public void copyResourcesToDwh(String resourceType, DwhFiles destDwh) throws IOException {
    Path path = fileSystem.getPath(getRoot(), resourceType);
    Path destPath = destDwh.getResourcePath(resourceType);
    log.info("Copying {} to {}", path, destPath);
    FileUtils.copyDirectory(path.toFile(), destPath.toFile());
  }

  public void writeTimestampFile() throws IOException {
    writeTimestampFile(Instant.now());
  }

  public void writeTimestampFile(Instant instant) throws IOException {
    Path filePath = fileSystem.getPath(getRoot(), TIMESTAMP_FILE);
    if (Files.exists(filePath)) {
      String errorMessage =
          String.format(
              "Attempting to write to the timestamp file %s which already exists", filePath);
      log.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    }
    Files.createDirectories(fileSystem.getPath(dwhRoot));
    Files.createFile(filePath);
    Files.write(filePath, instant.toString().getBytes(StandardCharsets.UTF_8));
  }

  public Instant readTimestampFile() throws IOException {
    Path filePath = fileSystem.getPath(getRoot(), TIMESTAMP_FILE);
    List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
    if (lines.isEmpty()) {
      String errorMessage = String.format("The timestamp file %s is empty", filePath);
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
    return Instant.parse(lines.get(0));
  }
}
