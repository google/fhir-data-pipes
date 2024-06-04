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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.fhir.analytics.exception.BulkExportException;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.fhir.analytics.model.BulkExportResponse.Output;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class containing methods to operate on Bulk Export APIs in the FHIR server */
public class BulkExportUtil {

  private static final Logger logger = LoggerFactory.getLogger(BulkExportUtil.class.getName());

  protected static final String TEMP_BULK_EXPORT_PATH_PREFIX = "bulk-export";

  protected static final int NUM_OF_PARALLEL_DOWNLOADS = 4;

  /**
   * This method triggers the Bulk export job in the FHIR server for the given resource types and
   * fhirVersionEnum. It then waits for the job to complete and then downloads the ndjson files
   * created by the bulk job into the local file system. The files are downloaded into paths of the
   * format <TempDir><File_Separator><ResourceType> i.e. all files for the same resource type are
   * downloaded into the same directory. These unique paths are then returned to the caller.
   *
   * @param resourceTypes - the resource types to be downloaded
   * @param fhirVersionEnum - the fhir version of resource types
   * @param fhirSearchUtil - the client to be used for invoking fhir server
   * @return the list of directory paths where the ndjson files are downloaded
   * @throws BulkExportException
   * @throws IOException
   */
  public static List<Path> triggerBulkExportAndDownloadFiles(
      List<String> resourceTypes, FhirVersionEnum fhirVersionEnum, FhirSearchUtil fhirSearchUtil)
      throws BulkExportException, IOException {
    Preconditions.checkState(!CollectionUtils.isEmpty(resourceTypes));
    Preconditions.checkNotNull(fhirVersionEnum);
    String contentLocationUrl = fhirSearchUtil.triggerBulkExportJob(resourceTypes, fhirVersionEnum);
    logger.info("Bulk Export has been started, contentLocationUrl={}", contentLocationUrl);
    BulkExportResponse bulkExportResponse = pollBulkExportJob(contentLocationUrl, fhirSearchUtil);
    if (!CollectionUtils.isEmpty(bulkExportResponse.getError())) {
      logger.error("Error occurred during bulk export, error={}", bulkExportResponse.getError());
      throw new BulkExportException("Error occurred during bulk export, please check logs");
    }

    if (CollectionUtils.isEmpty(bulkExportResponse.getOutput())
        && CollectionUtils.isEmpty(bulkExportResponse.getDeleted())) {
      logger.warn("No resources found to be exported!");
      return null;
    }
    List<Path> paths = new ArrayList<>();
    if (!CollectionUtils.isEmpty(bulkExportResponse.getDeleted())) {
      // TODO : Delete the FHIR resources
    }
    if (!CollectionUtils.isEmpty(bulkExportResponse.getOutput())) {
      Path tempDir = Files.createTempDirectory(TEMP_BULK_EXPORT_PATH_PREFIX);
      paths.addAll(downloadFilesToLocalFileSystem(tempDir, bulkExportResponse.getOutput()));
    }
    return paths;
  }

  private static List<Path> downloadFilesToLocalFileSystem(
      Path localPath, List<Output> resourceFiles) throws IOException, BulkExportException {
    Set<Path> paths = new HashSet<>();
    logger.info("Downloading files into local file system at path = {}", localPath);
    Map<String, String> urlToFileMapping = new HashMap<>();
    FileUtils.forceMkdir(localPath.toFile());
    for (Output output : resourceFiles) {
      Path resourceDir = Path.of(localPath.toString(), output.getType());
      FileUtils.forceMkdir(resourceDir.toFile());
      paths.add(resourceDir);
      String url = output.getUrl();
      String fileName = url.lastIndexOf("/") >= 0 ? url.substring(url.lastIndexOf("/") + 1) : url;
      String resourceFileName = resourceDir.resolve(fileName).toString();
      urlToFileMapping.put(output.getUrl(), resourceFileName);
    }
    downloadFiles(urlToFileMapping);
    logger.info("Downloading of files complete");
    return paths.stream().toList();
  }

  /**
   * Downloads each url file to the location it has been mapped to in the given urlToFileMapping.
   * The files are downloaded using a fixed thread pool and the current thread gets blocked until
   * all the files are downloaded or even if one of them fails in which case BulkExportException is
   * returned to the caller.
   */
  private static void downloadFiles(Map<String, String> urlToFileMapping)
      throws BulkExportException {
    if (urlToFileMapping != null && !urlToFileMapping.isEmpty()) {
      ExecutorService executor = null;
      List<Future> futureList = new ArrayList<>();
      try {
        executor = Executors.newFixedThreadPool(NUM_OF_PARALLEL_DOWNLOADS);
        for (String url : urlToFileMapping.keySet()) {
          futureList.add(executor.submit(new DownloadTask(url, urlToFileMapping.get(url))));
        }
        // Wait for all downloads to complete
        for (Future future : futureList) {
          future.get();
        }
      } catch (ExecutionException e) {
        logger.error("Error while downloading files", e);
        throw new BulkExportException(e.getMessage());
      } catch (InterruptedException e) {
        logger.error(
            "Caught InterruptedException; resetting interrupt flag and throwing RuntimeException! ",
            e);
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        if (executor != null) {
          executor.shutdown();
        }
      }
    }
  }

  /**
   * This method keeps polling for the status of bulk export job until it is complete and returns
   * the status once completed.
   */
  private static BulkExportResponse pollBulkExportJob(
      String bulkExportStatusUrl, FhirSearchUtil fhirSearchUtil)
      throws BulkExportException, IOException {
    // TODO : Add timeout here to avoid infinite polling
    while (true) {
      BulkExportHttpResponse bulkExportHttpResponse =
          fhirSearchUtil.fetchBulkExportHttpResponse(bulkExportStatusUrl);
      if (bulkExportHttpResponse.getHttpStatus() == HttpStatus.SC_OK) {
        logger.info("Bulk Export job is complete");
        return bulkExportHttpResponse.getBulkExportResponse();
      } else if (bulkExportHttpResponse.getHttpStatus() == HttpStatus.SC_ACCEPTED
          || bulkExportHttpResponse.getHttpStatus() == 429) { // Server is busy
        int retryAfterInMillis =
            bulkExportHttpResponse.getRetryAfter() > 0
                ? bulkExportHttpResponse.getRetryAfter() * 1000
                : 60000;
        logger.info(
            "Export job is still in progress, will check again in {} secs",
            retryAfterInMillis / 1000);
        try {
          Thread.sleep(retryAfterInMillis);
        } catch (InterruptedException e) {
          logger.error(
              "Caught InterruptedException; resetting interrupt flag and throwing "
                  + "RuntimeException! ",
              e);
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } else {
        logger.error(
            "Error while checking the status of the bulk export job, httpStatus={}, response={}",
            bulkExportHttpResponse.getHttpStatus(),
            bulkExportHttpResponse);
        throw new BulkExportException(
            "Error while checking the status of the bulk export job, please check logs for more"
                + " details");
      }
    }
  }

  private static class DownloadTask implements Runnable {

    private String sourceUrl;
    private String targetFile;

    public DownloadTask(String sourceUrl, String targetFile) {
      Preconditions.checkState(!Strings.isNullOrEmpty(sourceUrl), "sourceUrl cannot be empty");
      Preconditions.checkNotNull(targetFile, "targetFileName cannot be empty");
      this.sourceUrl = sourceUrl;
      this.targetFile = targetFile;
    }

    @Override
    public void run() {
      try {
        Files.createFile(Path.of(targetFile));
        try (ReadableByteChannel readableByteChannel =
                Channels.newChannel(new URL(sourceUrl).openConnection().getInputStream());
            FileChannel fileChannel = new FileOutputStream(targetFile).getChannel()) {
          fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        }
      } catch (IOException e) {
        e.printStackTrace();
        logger.error(
            "Error while downloading file from source url={} to targetFile={}, error={}",
            sourceUrl,
            targetFile,
            e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }
}
