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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import com.cerner.bunsen.FhirContexts;
import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interface for working with the data-warehouse files. This is where all file-structure logic
 * should be implemented. This should support different file-systems, including distributed ones.
 */
public class DwhFiles {

  private static final Logger log = LoggerFactory.getLogger(DwhFiles.class);

  public static final String TIMESTAMP_FILE_START = "timestamp_start.txt";

  public static final String TIMESTAMP_FILE_END = "timestamp_end.txt";

  private static final String INCREMENTAL_DIR = "incremental_run";

  private static final Pattern FILE_SCHEME_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*):/.*");

  static final String DEFAULT_SCHEME = "file";

  private final String dwhRoot;

  private final FhirContext fhirContext;

  private DwhFiles(String dwhRoot) {
    this(dwhRoot, FhirContexts.forR4());
  }

  /**
   * In order to interpret the file paths correctly the {@code dwhRoot} has to be represented in one
   * of the below formats according to the filesystem platform:
   *
   * <p>Linux/Mac:
   *
   * <ul>
   *   <li>TestRoot/SamplePrefix
   *   <li>/Users/beam/TestRoot/SamplePrefix
   * </ul>
   *
   * <p>Windows OS:
   *
   * <ul>
   *   <li>TestRoot\SamplePrefix
   *   <li>C:\Users\beam\TestRoot\SamplePrefix
   * </ul>
   *
   * <p>GCS filesystem
   *
   * <ul>
   *   <li>gs://TestBucket/TestRoot/SamplePrefix
   * </ul>
   *
   * @param dwhRoot
   * @param fhirContext
   */
  DwhFiles(String dwhRoot, FhirContext fhirContext) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dwhRoot));
    this.dwhRoot = dwhRoot;
    this.fhirContext = fhirContext;
  }

  static DwhFiles forRoot(String dwhRoot) {
    return new DwhFiles(dwhRoot);
  }

  public String getRoot() {
    return dwhRoot;
  }

  /**
   * This returns the path to the directory which contains resources of a specific type.
   *
   * @param resourceType the type of the FHIR resources
   * @return The path to the resource type dir
   */
  public ResourceId getResourcePath(String resourceType) {
    return FileSystems.matchNewResource(getRoot(), true)
        .resolve(resourceType, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * @param resourceType the type of the FHIR resources
   * @return The file pattern for Parquet files of `resourceType` in this DWH.
   */
  public String getFilePattern(String resourceType) {
    return String.format(
        "%s*%s", getResourcePath(resourceType).toString(), ParquetUtil.PARQUET_EXTENSION);
  }

  /**
   * This returns the default incremental run path; each incremental run is relative to a full path,
   * hence we put this directory under the full-run root.
   *
   * @return the default incremental run path
   */
  public ResourceId getIncrementalRunPath() {
    return FileSystems.matchNewResource(getRoot(), true)
        .resolve(INCREMENTAL_DIR, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /** This is used when we want to keep a backup of the old incremental run output. */
  public ResourceId getIncrementalRunPathWithTimestamp() {
    return FileSystems.matchNewResource(getRoot(), true)
        .resolve(
            String.format("%s_old_%d", INCREMENTAL_DIR, System.currentTimeMillis()),
            StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * Similar to {@link #getIncrementalRunPath} but also checks if that directory exists and if so,
   * moves it to {@link #getIncrementalRunPathWithTimestamp()}.
   *
   * @return same as {@link #getIncrementalRunPath()}
   * @throws IOException if the directory move fails
   */
  public ResourceId newIncrementalRunPath() throws IOException {
    ResourceId incPath = getIncrementalRunPath();
    if (hasIncrementalDir()) {
      ResourceId movePath = getIncrementalRunPathWithTimestamp();
      log.info("Moving the old {} directory to {}", INCREMENTAL_DIR, movePath);
      FileSystems.rename(Collections.singletonList(incPath), Collections.singletonList(movePath));
    }
    return incPath;
  }

  /**
   * @return true iff there is already an incremental run subdirectory in this DWH.
   */
  public boolean hasIncrementalDir() throws IOException {
    List<MatchResult> matches =
        FileSystems.matchResources(Collections.singletonList(getIncrementalRunPath()));
    MatchResult matchResult = Iterables.getOnlyElement(matches);
    return matchResult.status() == Status.OK;
  }

  /**
   * This method returns the list of non-empty directories which contains at least one file under
   * it. The directory name should be a valid FHIR resource type.
   *
   * @return the set of non-empty directories
   * @throws IOException
   */
  public Set<String> findNonEmptyFhirResourceTypes() throws IOException {
    // TODO : If the list of files under the dwhRoot is huge then there can be a lag in the api
    //  response. This issue https://github.com/google/fhir-data-pipes/issues/288 helps in
    //  maintaining the number of file to an optimum value.
    String fileSeparator = getFileSeparatorForDwhFiles(dwhRoot);
    List<MatchResult> matchedChildResultList =
        FileSystems.match(
            List.of(
                getPathEndingWithFileSeparator(dwhRoot, fileSeparator)
                    + "*"
                    + fileSeparator
                    + "*"));
    Set<String> fileSet = new HashSet<>();
    for (MatchResult matchResult : matchedChildResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          fileSet.add(metadata.resourceId().getCurrentDirectory().getFilename());
        }
      } else if (matchResult.status() == Status.ERROR) {
        log.error("Error matching resource types under {} ", dwhRoot);
        throw new IOException(String.format("Error matching resource types under %s", dwhRoot));
      }
    }

    Set<String> typeSet = Sets.newHashSet();
    for (String file : fileSet) {
      try {
        fhirContext.getResourceType(file);
        typeSet.add(file);
      } catch (DataFormatException e) {
        log.debug("Ignoring file {} which is not a FHIR resource.", file);
      }
    }
    log.info("Resource types under {} are {}", dwhRoot, typeSet);
    return typeSet;
  }

  /**
   * This method copies all the files under the directory (getRoot() + resourceType) into the
   * destination DwhFiles under the similar directory.
   *
   * @param resourceType
   * @param destDwh
   * @throws IOException
   */
  public void copyResourcesToDwh(String resourceType, DwhFiles destDwh) throws IOException {
    String fileSeparator = getFileSeparatorForDwhFiles(dwhRoot);
    List<MatchResult> sourceMatchResultList =
        FileSystems.match(
            List.of(
                getPathEndingWithFileSeparator(dwhRoot, fileSeparator)
                    + resourceType
                    + fileSeparator
                    + "*"));
    List<ResourceId> sourceResourceIdList = new ArrayList<>();

    // The sourceMatchResultList should only contain 1 element as the matching list
    // (List.of(sourceFiles)) used for it was also one. The actual matched files should be
    // retrieved from the matchResult.metadata element which could be more than one
    for (MatchResult matchResult : sourceMatchResultList) {
      if (matchResult.status() == Status.OK) {
        List<ResourceId> resourceIds =
            matchResult.metadata().stream()
                .map(metadata -> metadata.resourceId())
                .collect(Collectors.toList());
        sourceResourceIdList.addAll(resourceIds);
      }
    }

    List<ResourceId> destResourceIdList = new ArrayList<>();
    sourceResourceIdList.stream()
        .forEach(
            resourceId -> {
              ResourceId destResourceId =
                  FileSystems.matchNewResource(destDwh.getRoot(), true)
                      .resolve(resourceType, StandardResolveOptions.RESOLVE_DIRECTORY)
                      .resolve(resourceId.getFilename(), StandardResolveOptions.RESOLVE_FILE);
              destResourceIdList.add(destResourceId);
            });
    FileSystems.copy(sourceResourceIdList, destResourceIdList);
  }

  public void writeTimestampFile(String fileName) throws IOException {
    writeTimestampFile(Instant.now(), fileName);
  }

  public void writeTimestampFile(Instant instant, String fileName) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matches = FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matches);

    if (matchResult.status() == Status.OK) {
      String errorMessage =
          String.format(
              "Attempting to write to the timestamp file %s which already exists",
              getRoot() + "/" + fileName);
      log.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    } else if (matchResult.status() == Status.NOT_FOUND) {
      WritableByteChannel writableByteChannel = FileSystems.create(resourceId, MimeTypes.BINARY);
      writableByteChannel.write(
          ByteBuffer.wrap(instant.toString().getBytes(StandardCharsets.UTF_8)));
      writableByteChannel.close();
    } else {
      String errorMessage =
          String.format(
              "Error matching file spec %s: status %s",
              getRoot() + "/" + fileName, matchResult.status());
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  public Instant readTimestampFile(String fileName) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);
    List<String> result = new ArrayList<>();
    ReadableByteChannel channel = FileSystems.open(resourceId);
    try (InputStream stream = Channels.newInputStream(channel)) {
      BufferedReader streamReader =
          new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      String line;
      while ((line = streamReader.readLine()) != null) {
        result.add(line);
      }
    }
    if (result.isEmpty()) {
      String errorMessage =
          String.format("The timestamp file %s is empty", getRoot() + "/" + fileName);
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
    return Instant.parse(result.get(0));
  }

  public void writeToFile(String fileName, byte[] content) throws IOException {
    ResourceId resourceId =
        FileSystems.matchNewResource(getRoot(), true)
            .resolve(fileName, StandardResolveOptions.RESOLVE_FILE);

    List<MatchResult> matches = FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matches);

    if (matchResult.status() == Status.OK) {
      String errorMessage =
          String.format(
              "Attempting to write to the file %s which already exists",
              getRoot() + "/" + fileName);
      log.error(errorMessage);
      throw new FileAlreadyExistsException(errorMessage);
    } else if (matchResult.status() == Status.NOT_FOUND) {
      WritableByteChannel writableByteChannel = FileSystems.create(resourceId, MimeTypes.BINARY);
      writableByteChannel.write(ByteBuffer.wrap(content));
      writableByteChannel.close();
    } else {
      String errorMessage =
          String.format(
              "Error matching file spec %s: status %s",
              getRoot() + "/" + fileName, matchResult.status());
      log.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  /**
   * This method returns the file separator for the filesystem where the data-warehouse is hosted.
   *
   * @param dwhRootPrefix the root directory prefix of the filesystem.
   * @return the file separator
   */
  public static String getFileSeparatorForDwhFiles(String dwhRootPrefix) {
    String scheme = parseScheme(dwhRootPrefix);
    switch (scheme) {
      case DEFAULT_SCHEME:
        return File.separator;
      case GcsPath.SCHEME:
        return "/";
      default:
        String errorMessage = String.format("File system scheme=%s is not yet supported", scheme);
        log.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
  }

  /** This method returns the {@code path} by appending the {@code fileseparator} if required. */
  public static String getPathEndingWithFileSeparator(String path, String fileSeparator) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(fileSeparator);
    return path.endsWith(fileSeparator) ? path : path + fileSeparator;
  }

  /**
   * This method returns the schema of the passed in spec. This is inspired from the parsing logic
   * in the repo <a href="https://github.com/apache/beam">Beam File System</a>
   *
   * @param spec - the spec from which the schema needs to be parsed
   * @return the schema
   */
  public static String parseScheme(String spec) {
    // The spec is almost, but not quite, a URI. In particular,
    // the reserved characters '[', ']', and '?' have meanings that differ
    // from their use in the URI spec. ('*' is not reserved).
    // Here, we just need the scheme, which is so circumscribed as to be
    // very easy to extract with a regex.
    Matcher matcher = FILE_SCHEME_PATTERN.matcher(spec);

    if (!matcher.matches()) {
      return DEFAULT_SCHEME;
    } else {
      return matcher.group("scheme").toLowerCase();
    }
  }
}
