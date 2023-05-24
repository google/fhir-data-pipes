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

import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.openmrs.analytics.metrics.ProgressStats;
import org.openmrs.analytics.metrics.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApiController {

  private static final Logger logger = LoggerFactory.getLogger(ApiController.class.getName());

  private static final String SUCCESS = "SUCCESS";

  @Autowired private PipelineManager pipelineManager;

  @PostMapping("/run")
  public ResponseEntity<String> runBatch(
      @RequestParam(name = "isFullRun", required = true) boolean isFullRun) {
    if (pipelineManager.isRunning()) {
      return new ResponseEntity<>("Another pipeline is running.", HttpStatus.BAD_REQUEST);
    }
    logger.info("Received request to start the pipeline ...");
    try {
      if (isFullRun) {
        pipelineManager.runBatchPipeline();
      } else {
        pipelineManager.runIncrementalPipeline();
      }
    } catch (Exception e) {
      logger.error("Error in starting the pipeline", e);
      return new ResponseEntity<>(
          "An unknown error has occurred.", HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return new ResponseEntity<>(SUCCESS, HttpStatus.OK);
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

  private Stats getStats() {
    MetricQueryResults metricQueryResults = pipelineManager.getMetricQueryResults();
    return Stats.createStats(metricQueryResults);
  }
}
