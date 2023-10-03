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

import static org.apache.beam.sdk.io.FileSystems.DEFAULT_SCHEME;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

/**
 * This class is responsible for purging the DWH snapshots when they expire. It also exposes few
 * helper methods to access the DWH files and directories.
 */
@EnableScheduling
@Component
public class DwhFilesManager {

  private final DataProperties dataProperties;

  private CronExpression purgeCron;

  private LocalDateTime lastPurgeRunEnd;

  private boolean isPurgeJobRunning;

  private String dwhRootPrefix;
  private int numOfDwhSnapshotsToRetain;

  private static final Logger logger = LoggerFactory.getLogger(DwhFilesManager.class.getName());

  @Autowired
  public DwhFilesManager(DataProperties dataProperties) {
    this.dataProperties = dataProperties;
  }

  @PostConstruct
  public void init() {
    purgeCron = CronExpression.parse(dataProperties.getPurgeSchedule());
    dwhRootPrefix = dataProperties.getDwhRootPrefix();
    Preconditions.checkState(dwhRootPrefix != null && !dwhRootPrefix.isEmpty());
    numOfDwhSnapshotsToRetain = dataProperties.getNumOfDwhSnapshotsToRetain();
    Preconditions.checkState(
        numOfDwhSnapshotsToRetain > 0, "numOfDwhSnapshotsToRetain should be positive value");
  }

  private synchronized boolean lockPurgeJob() {
    if (isPurgeJobRunning) {
      // Already the job is running so lock can't be acquired.
      return false;
    }
    isPurgeJobRunning = true;
    return true;
  }

  private synchronized void releasePurgeJob() {
    isPurgeJobRunning = false;
  }

  /**
   * @return the next scheduled time to run the purge job based on the previous run time.
   */
  LocalDateTime getNextPurgeTime() {
    if (lastPurgeRunEnd == null) {
      return LocalDateTime.now();
    }
    return purgeCron.next(lastPurgeRunEnd);
  }

  // Every 5 minutes, check if the purge job needs to be triggered.
  @Scheduled(fixedDelay = 300000)
  void checkPurgeScheduleAndTrigger() {
    try {
      if (!lockPurgeJob()) {
        return;
      }
      LocalDateTime next = getNextPurgeTime();
      logger.info("Last purge run was at {} next run is at {}", lastPurgeRunEnd, next);
      if (next.compareTo(LocalDateTime.now()) <= 0) {
        logger.info("Purge run triggered at {}", LocalDateTime.now());
        purgeDwhFiles();
        logger.info("Purge run completed at {}", LocalDateTime.now());
      }
    } finally {
      releasePurgeJob();
    }
  }

  /**
   * This method will scan the DWH base directory to check for any older DWH snapshots that needs to
   * be purged. It will retain the most recent successfully completed snapshots and purge the older
   * ones. Any incomplete snapshots will not be purged by this job and will have to be manually
   * verified and acted upon.
   */
  private void purgeDwhFiles() {
    String baseDir = getBaseDir(dwhRootPrefix);
    try {
      String prefix = getPrefix(dwhRootPrefix);
      List<ResourceId> paths =
          getAllChildDirectories(baseDir).stream()
              .filter(dir -> dir.getFilename().startsWith(prefix))
              .collect(Collectors.toList());

      TreeSet<String> recentSnapshotsToBeRetained =
          getRecentSnapshots(paths, numOfDwhSnapshotsToRetain);
      deleteOlderSnapshots(paths, recentSnapshotsToBeRetained);
      lastPurgeRunEnd = LocalDateTime.now();
    } catch (IOException e) {
      logger.error("Error occurred while purging older snapshots", e);
    }
  }

  private void deleteOlderSnapshots(
      List<ResourceId> allPaths, TreeSet<String> recentSnapshotsToBeRetained) throws IOException {
    for (ResourceId path : allPaths) {
      if (!recentSnapshotsToBeRetained.contains(path.getFilename()) && isDwhComplete(path)) {
        deleteDirectoryAndFiles(path);
      }
    }
  }

  /**
   * This method deletes all the files, directories and subdirectories under the given root
   * directory. This is done by first deleting all the files under the root directory including
   * under the subdirectories. Later the directories and the subdirectories are deleted.
   *
   * @param rootDirectory which needs to be deleted
   * @throws IOException
   */
  private void deleteDirectoryAndFiles(ResourceId rootDirectory) throws IOException {
    ResourceId filesResourceToBeMatched =
        rootDirectory.resolve("**", StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matchedFilesResultList =
        FileSystems.matchResources(Collections.singletonList(filesResourceToBeMatched));

    // Collect the matched files which also includes the files under the subdirectories, at the same
    // time collect the directories as well.
    List<ResourceId> fileResources = new ArrayList<>();
    Set<String> allDirectoryPathSet = new HashSet<>();
    for (MatchResult matchResult : matchedFilesResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          fileResources.add(metadata.resourceId());
          ResourceId currentDirectory = metadata.resourceId().getCurrentDirectory();
          allDirectoryPathSet.add(currentDirectory.toString());
          // Beam currently creates a temporary empty folder with name '.temp-beam' sometimes under
          // the directories. These empty subdirectories could not be determined with the existing
          // Beam file system apis, hence this is hard-coded here so that these empty directories
          // and their parent directories can be successfully deleted.
          allDirectoryPathSet.add(
              currentDirectory
                  .resolve(".temp-beam", StandardResolveOptions.RESOLVE_DIRECTORY)
                  .toString());
        }
      } else if (matchResult.status() == Status.ERROR) {
        String errorMessage =
            String.format("Error matching files under directory %s", rootDirectory.getFilename());
        logger.error(errorMessage);
        throw new IOException(errorMessage);
      }
    }
    List<String> allDirectoryPathList = new ArrayList<>(allDirectoryPathSet);
    // Sorting the directories in reverse order will make sure that the child directories will
    // appear before their corresponding parent directories in the list and the deletion will happen
    // in the same order i.e. start with the leaf child node and then with their respective parent
    // directories.
    allDirectoryPathList.sort(Collections.reverseOrder());
    List<ResourceId> directoryResourceIdList =
        allDirectoryPathList.stream()
            .map(directory -> FileSystems.matchNewResource(directory, true))
            .collect(Collectors.toList());
    directoryResourceIdList.add(rootDirectory);

    // Delete the files and directories
    try {
      FileSystems.delete(fileResources);
      FileSystems.delete(directoryResourceIdList);
    } catch (DirectoryNotEmptyException e) {
      logger.warn(
          "Error while deleting a directory, ignoring this and resuming further. error={}",
          e.getMessage());
    }
  }

  private TreeSet<String> getRecentSnapshots(List<ResourceId> paths, int numberOfSnapshotsToReturn)
      throws IOException {
    TreeSet<String> treeSet = new TreeSet<>();
    for (ResourceId resourceId : paths) {
      // Ignore incomplete DWH snapshots
      if (!isDwhComplete(resourceId)) {
        continue;
      }
      // Keep only the recent snapshots
      if (treeSet.size() < numberOfSnapshotsToReturn) {
        treeSet.add(resourceId.getFilename());
      } else if (resourceId.getFilename().compareTo(treeSet.first()) > 0) {
        treeSet.add(resourceId.getFilename());
        treeSet.pollFirst();
      }
    }
    return treeSet;
  }

  /**
   * This method tells us whether the DWH snapshot has been successfully completed by the pipeline.
   * This is determined by checking the existence of both the timestamp files which needs to be
   * created at the start and end of the pipeline runs.
   *
   * @param dwhResource The DWH resource path which needs to be checked for completeness
   * @return the status of DWH completeness.
   * @throws IOException
   */
  boolean isDwhComplete(ResourceId dwhResource) throws IOException {
    ResourceId startTimestampResource =
        dwhResource.resolve(DwhFiles.TIMESTAMP_FILE_START, StandardResolveOptions.RESOLVE_FILE);
    ResourceId endTimestampResource =
        dwhResource.resolve(DwhFiles.TIMESTAMP_FILE_END, StandardResolveOptions.RESOLVE_FILE);
    return (doesFileExist(startTimestampResource) && doesFileExist(endTimestampResource));
  }

  private boolean doesFileExist(ResourceId resourceId) throws IOException {
    List<MatchResult> matchResultList =
        FileSystems.matchResources(Collections.singletonList(resourceId));
    MatchResult matchResult = Iterables.getOnlyElement(matchResultList);
    if (matchResult.status() == Status.OK) {
      return true;
    } else if (matchResult.status() == Status.NOT_FOUND) {
      return false;
    } else {
      String errorMessage =
          String.format("Error matching file spec %s: status %s", resourceId, matchResult.status());
      logger.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  /**
   * This method returns the base directory where the DWH snapshots needs to be created. This is
   * determined by ignoring the prefix part in the given input format <baseDir></prefix> for the
   * dwhRootPrefix
   *
   * @param dwhRootPrefix
   * @return the base directory name
   */
  String getBaseDir(String dwhRootPrefix) {
    int index = getLastIndexOfSlash(dwhRootPrefix);
    if (index <= 0) {
      String errorMessage =
          "dwhRootPrefix should be configured with a non-empty base directory. It should be of the"
              + " format <baseDir>/<prefix>";
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    return dwhRootPrefix.substring(0, index);
  }

  /**
   * This method returns the prefix name that needs to be applied to the DWH snapshot root folder
   * name. This is determined by considering the prefix part in the given input format
   * <baseDir></prefix> for the dwhRootPrefix
   *
   * @param dwhRootPrefix
   * @return the prefix name
   */
  String getPrefix(String dwhRootPrefix) {
    int index = getLastIndexOfSlash(dwhRootPrefix);
    if (index <= 0) {
      String errorMessage =
          "dwhRootPrefix should be configured with a non-empty base directory. It should be of the"
              + " format <baseDir>/<prefix>";
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    String prefix = dwhRootPrefix.substring(index + 1);
    if (prefix == null || prefix.isBlank()) {
      String errorMessage =
          "dwhRootPrefix should be configured with a non-empty suffix string after the last"
              + " occurrence of the character '/'";
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    return prefix;
  }

  List<String> findExistingResources(String dwhRoot) throws IOException {
    Set<ResourceId> childPaths = getAllChildDirectories(dwhRoot);
    Set<String> configuredSet =
        new HashSet<>(Arrays.asList(dataProperties.getResourceList().split(",")));
    return childPaths.stream()
        .map(r -> r.getFilename())
        .filter(r -> configuredSet.contains(r))
        .collect(Collectors.toList());
  }

  /**
   * Returns all the child directories under the given base directory which are 1-level deep.
   *
   * @param baseDir
   * @return The list of all child directories under the base directory
   * @throws IOException
   */
  Set<ResourceId> getAllChildDirectories(String baseDir) throws IOException {
    // TODO avoid using `/` in resolve.
    ResourceId resourceId =
        FileSystems.matchNewResource(baseDir, true)
            .resolve("*/*", StandardResolveOptions.RESOLVE_FILE);
    List<MatchResult> matchResultList =
        FileSystems.matchResources(Collections.singletonList(resourceId));
    Set<ResourceId> childDirectories = new HashSet<>();
    for (MatchResult matchResult : matchResultList) {
      if (matchResult.status() == Status.OK && !matchResult.metadata().isEmpty()) {
        for (Metadata metadata : matchResult.metadata()) {
          childDirectories.add(metadata.resourceId().getCurrentDirectory());
        }
      } else if (matchResult.status() == Status.ERROR) {
        String errorMessage = String.format("Error matching files under directory %s", baseDir);
        logger.error(errorMessage);
        throw new IOException(errorMessage);
      }
    }
    logger.info("Child directories : {}", childDirectories);
    return childDirectories;
  }

  private int getLastIndexOfSlash(String dwhRootPrefix) {
    String scheme = parseScheme(dwhRootPrefix);
    int index = -1;
    switch (scheme) {
      case DEFAULT_SCHEME:
        index = dwhRootPrefix.lastIndexOf("/");
        break;
      case GcsPath.SCHEME:
        // Fetch the last index position of the character '/' after the bucket name in the gcs path.
        GcsPath gcsPath = GcsPath.fromUri(dwhRootPrefix);
        String gcsObject = gcsPath.getObject();
        if (Strings.isNullOrEmpty(gcsObject)) {
          break;
        }
        int position = gcsObject.lastIndexOf("/");
        if (position == -1) {
          index = dwhRootPrefix.lastIndexOf(gcsObject) - 1;
        } else {
          index = dwhRootPrefix.lastIndexOf(gcsObject) + position;
        }
        break;
      default:
        String errorMessage = String.format("File system scheme=%s is not yet supported", scheme);
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
    return index;
  }

  private static String parseScheme(String spec) {
    URI uri = URI.create(spec);
    return Strings.isNullOrEmpty(uri.getScheme()) ? DEFAULT_SCHEME : uri.getScheme();
  }
}
