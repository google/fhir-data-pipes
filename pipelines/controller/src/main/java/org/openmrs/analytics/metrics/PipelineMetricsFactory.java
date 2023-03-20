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
package org.openmrs.analytics.metrics;

import org.apache.beam.sdk.PipelineRunner;
import org.openmrs.analytics.exception.MetricsNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipelineMetricsFactory {

  private static final Logger logger =
      LoggerFactory.getLogger(PipelineMetricsFactory.class.getName());
  private static final String FLINK_RUNNER = "FlinkRunner";
  @Autowired private FlinkPipelineMetrics flinkPipelineMetrics;

  public PipelineMetrics getPipelineMetrics(Class<? extends PipelineRunner> pipelineRunner)
      throws MetricsNotSupportedException {
    switch (pipelineRunner.getSimpleName()) {
      case FLINK_RUNNER:
        return flinkPipelineMetrics;
      default:
        throw new MetricsNotSupportedException(
            "metrics not supported " + pipelineRunner.getClass().getSimpleName());
    }
  }
}
