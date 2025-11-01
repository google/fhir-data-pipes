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

import ca.uhn.fhir.parser.DataFormatException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.fhir.analytics.PipelineManager.DwhRunDetails;
import com.google.fhir.analytics.PipelineManager.RunMode;
import com.google.fhir.analytics.metrics.CumulativeMetrics;
import com.google.fhir.analytics.metrics.ProgressStats;
import com.google.fhir.analytics.metrics.Stats;
import com.google.fhir.analytics.view.ViewApplicationException;
import com.google.fhir.analytics.view.ViewApplicator;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import com.google.fhir.analytics.view.ViewDefinition;
import com.google.fhir.analytics.view.ViewDefinitionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiController {

  private static final Logger logger = LoggerFactory.getLogger(ApiController.class.getName());

  private static final String SUCCESS = "SUCCESS";

  @Autowired private PipelineManager pipelineManager;

  @Autowired private DataProperties dataProperties;

  @Autowired private DwhFilesManager dwhFilesManager;

  @PostMapping("/run")
  public ResponseEntity<Map<String,String>> runBatch(
      @RequestParam(name = "runMode", required = true) String runMode) {
    if (pipelineManager.isRunning()) {
      return new ResponseEntity<>(Collections.singletonMap("message","Another pipeline is running."), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    logger.info("Received request to start the pipeline ...");
    try {
      if (RunMode.FULL.name().equals(runMode)) {
        pipelineManager.runBatchPipeline(false);
      } else if (RunMode.INCREMENTAL.name().equals(runMode)) {
        pipelineManager.runIncrementalPipeline();
      } else if (RunMode.VIEWS.name().equals(runMode)) {
        pipelineManager.runBatchPipeline(true);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "The runType argument should be one of %s %s %s got %s",
                RunMode.FULL.name(), RunMode.INCREMENTAL.name(), RunMode.VIEWS.name(), runMode));
      }
    } catch (Exception e) {
      logger.error("Error in starting the pipeline", e);
      return new ResponseEntity<>(
              Collections.singletonMap("message","An unknown error has occurred."), HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return new ResponseEntity<>(Collections.singletonMap("message", SUCCESS), HttpStatus.OK);
  }

  @GetMapping("/status")
  public ProgressStats getStatus() {
    ProgressStats progressStats = new ProgressStats();
    if (pipelineManager.isRunning()) {
      progressStats.setPipelineStatus("RUNNING");
      progressStats.setStats(getStats());
    } else {
      progressStats.setPipelineStatus("IDLE");
    }
    return progressStats;
  }

  @PostMapping("/tables")
  public String createResourceTables() {
    if (pipelineManager.isRunning()) {
      throw new IllegalStateException("Cannot create tables because another pipeline is running!");
    }
    logger.info("Received request to create request tables ...");
    pipelineManager.createResourceTables();
    return SUCCESS;
  }

  @Nullable
  private Stats getStats() {
    CumulativeMetrics cumulativeMetrics = pipelineManager.getCumulativeMetrics();
    return Stats.createStats(cumulativeMetrics);
  }

  /** If available, fetches the error log file for the latest pipeline run. */
  @GetMapping(
      value = "/download-error-log",
      produces = {MediaType.TEXT_PLAIN_VALUE})
  public ResponseEntity<InputStreamResource> downloadErrorLog() throws IOException {
    DwhRunDetails lastRunDetails = pipelineManager.getLastRunDetails();
    if (lastRunDetails == null) {
      throw new IllegalStateException("No pipelines have been run yet");
    }
    if (Strings.isNullOrEmpty(lastRunDetails.getErrorLogPath())) {
      throw new IllegalStateException("No error file exists for the last pipeline run");
    }

    ResourceId errorResource =
        FileSystems.matchNewResource(lastRunDetails.getErrorLogPath(), false);
    ReadableByteChannel channel = FileSystems.open(errorResource);
    InputStream stream = Channels.newInputStream(channel);
    InputStreamResource inputStreamResource = new InputStreamResource(stream);
    MultiValueMap<String, String> headers = new HttpHeaders();
    headers.put("Content-type", Arrays.asList(MediaType.TEXT_PLAIN_VALUE));
    return new ResponseEntity<>(inputStreamResource, headers, HttpStatus.OK);
  }

  @PostMapping("/test-view")
  public ResponseEntity<String> testView(
      @RequestParam(name = "resource", required = true) String resourceContent,
      @RequestParam(name = "viewDef", required = true) String viewDef) {
    logger.debug("viewDef: {}", viewDef);
    logger.debug("resourceContent: {}", resourceContent);
    String response = "";
    HttpStatus status = HttpStatus.OK;
    try {
      ViewDefinition view = ViewDefinition.createFromString(Strings.nullToEmpty(viewDef));
      ViewApplicator viewApplicator = new ViewApplicator(view);
      IBaseResource resource =
          pipelineManager
              .getFhirContext()
              .newJsonParser()
              .parseResource(Strings.nullToEmpty(resourceContent));
      RowList rows = viewApplicator.apply(resource);
      response = rows.toHtml();
    } catch (ViewDefinitionException e) {
      response = "View definition error: " + e.getMessage();
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    } catch (ViewApplicationException e) {
      response = "Error in applying view on the resource: " + e.getMessage();
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    } catch (DataFormatException e) {
      response = "Error in parsing the FHIR resource: " + e.getMessage();
      status = HttpStatus.INTERNAL_SERVER_ERROR;
    }
    return new ResponseEntity<>(response, status);
  }

  @GetMapping("/next")
  public ScheduleDto getNextScheduled() {
    ScheduleDto schedule = new ScheduleDto();
    LocalDateTime nextRun = pipelineManager.getNextIncrementalTime();
    if (nextRun == null) {
      schedule.setNextRun("NOT SCHEDULED");
    } else {
      schedule.setNextRun(nextRun.toString());
    }
    return schedule;
  }

  @GetMapping("/dwh")
  public DwhDto getDwh() {
    DwhDto dwhDto = new DwhDto();
    String dwh = pipelineManager.getCurrentDwhRoot();
    dwhDto.setDwhPrefix(dataProperties.getDwhRootPrefix());
    dwhDto.setDwhPath(dwh == null || dwh.isEmpty() ? "" : dwh);
    dwhDto.setDwhSnapshots(dwhFilesManager.listDwhSnapshots());
    return dwhDto;
  }

  @DeleteMapping("/dwh")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void deleteSnapshot(@RequestParam String snapshotId) throws IOException {
    dwhFilesManager.deleteDwhSnapshotFiles(snapshotId);
  }

  @GetMapping("/config")
  public Map<String, String> getConfigs() {
    return getConfigMap(null);
  }

  @GetMapping("/config/{name}")
  public Map<String, String> getConfigs(@PathVariable String name) {
    return getConfigMap(name);
  }

  private Map<String, String> getConfigMap(@Nullable String configName) {
    List<DataProperties.ConfigFields> configParams = dataProperties.getConfigParams();

    Map<String, String> configMap;
    Stream<DataProperties.ConfigFields> fieldsStream = configParams.stream();

    if (configName != null && !configName.isEmpty()) {
      fieldsStream = fieldsStream.filter(configField -> configField.name.equals(configName));
    }

    configMap =
        fieldsStream.collect(
            Collectors.toMap(
                configField -> configField.name,
                configField -> configField.value != null ? configField.value : ""));
    return configMap;
  }

  @SuppressWarnings("NullAway.Init")
  @Data
  public static class ScheduleDto {
    @JsonProperty("next_run")
    private String nextRun;
  }

  @SuppressWarnings("NullAway.Init")
  @Data
  public static class DwhDto {
    @JsonProperty("dwh_prefix")
    private String dwhPrefix;

    @JsonProperty("dwh_path_latest")
    private String dwhPath;

    @JsonProperty("dwh_snapshot_ids")
    private List<String> dwhSnapshots;
  }
}
