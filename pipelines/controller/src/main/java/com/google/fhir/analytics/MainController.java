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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class MainController {

  private static final Logger logger = LoggerFactory.getLogger(MainController.class.getName());

  private static final String HTML_INDEX_PAGE = "index";

  private static final String HTML_HELP_PAGE = "help";

  @Autowired private DataProperties dataProperties;

  @Autowired private PipelineManager pipelineManager;

  @GetMapping("/")
  public String index(Model model) throws IOException, PropertyVetoException {
    model.addAttribute("isRunning", pipelineManager.isRunning());
    String dwh = pipelineManager.getCurrentDwhRoot();
    if (dwh == null || dwh.isEmpty()) {
      model.addAttribute(
          "dwh",
          String.format(
              "No DWH found with %s prefix; 'Run Full Pipeline' to create one.",
              dataProperties.getDwhRootPrefix()));
    } else {
      model.addAttribute("dwh", dwh);
    }
    LocalDateTime next = pipelineManager.getNextIncrementalTime();
    if (next == null) {
      model.addAttribute("next_run", "NOT SCHEDULED");
      model.addAttribute("hasDwh", false);
    } else {
      model.addAttribute("next_run", next.toString());
      model.addAttribute("hasDwh", true);
    }
    FhirEtlOptions options = dataProperties.createBatchOptions().getFhirEtlOptions();
    List<DataProperties.ConfigFields> configParams = dataProperties.getConfigParams();
    model.addAttribute("configParams", configParams);
    List<DataProperties.ConfigFields> pipelineConfigs = dataProperties.getConfigFieldsList(options);
    model.addAttribute(
        "nonDefConfigs",
        pipelineConfigs.stream().filter(c -> !c.def.equals(c.value)).collect(Collectors.toList()));
    model.addAttribute(
        "defConfigs",
        pipelineConfigs.stream().filter(c -> c.def.equals(c.value)).collect(Collectors.toList()));
    model.addAttribute("lastRunDetails", pipelineManager.getLastRunDetails());
    logger.info("Done serving /");
    return HTML_INDEX_PAGE;
  }

  @GetMapping("/help")
  public String config(Model model) {
    return HTML_HELP_PAGE;
  }
}
