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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.flink.core.execution.JobClient;
import org.openmrs.analytics.MetricsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class FlinkPipelineMetrics implements PipelineMetrics {

  private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineMetrics.class);

  private static volatile JobClient jobClient;

  /**
   * This method returns the MetricQueryResults for the currently running pipeline. The current
   * running pipeline is determined by the JobClient which is set by the FlinkJobListener when the
   * pipeline is started.
   *
   * @return MetricQueryResults
   */
  @Override
  public MetricQueryResults getMetricQueryResults() {
    if (jobClient == null) {
      return getEmptyMetricQueryResults();
    }

    try {
      // Blocking call to get the accumulators
      Map<String, Object> accumulators = jobClient.getAccumulators().get();
      MetricsContainerStepMap metricsContainerStepMap = null;
      if (accumulators != null && !accumulators.isEmpty()) {
        metricsContainerStepMap =
            (MetricsContainerStepMap) accumulators.get(FlinkMetricContainer.ACCUMULATOR_NAME);
      }
      MetricResults metricResults = null;
      if (metricsContainerStepMap != null) {
        metricResults =
            MetricsContainerStepMap.asAttemptedOnlyMetricResults(metricsContainerStepMap);
      }

      if (metricResults != null) {
        return metricResults.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.inNamespace(MetricsConstants.METRICS_NAMESPACE))
                .build());
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return getEmptyMetricQueryResults();
  }

  private MetricQueryResults getEmptyMetricQueryResults() {
    return MetricQueryResults.create(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }

  public static synchronized void setJobClient(JobClient jobClient) {
    FlinkPipelineMetrics.jobClient = jobClient;
  }

  public static synchronized void clearJobClient() {
    FlinkPipelineMetrics.jobClient = null;
  }
}
