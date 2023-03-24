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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
  public String runBatch(@RequestParam(name = "isFullRun", required = true) boolean isFullRun)
      throws IOException, PropertyVetoException, SQLException {
    if (pipelineManager.isRunning()) {
      throw new IllegalStateException("Another pipeline is running!");
    }
    logger.info("Received request to start the pipeline ...");
    if (isFullRun) {
      pipelineManager.runBatchPipeline();
    } else {
      pipelineManager.runIncrementalPipeline();
    }
    return "SUCCESS";
  }

  @GetMapping("/status")
  public String getStatus() {
    // TODO instead of just a boolean status, we should return progress stats too.
    if (pipelineManager.isRunning()) {
      return "RUNNING";
    }
    return "IDLE";
  }
}
