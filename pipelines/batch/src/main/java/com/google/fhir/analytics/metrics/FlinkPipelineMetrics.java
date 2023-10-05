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
package com.google.fhir.analytics.metrics;

import com.google.fhir.analytics.MetricsConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.flink.core.execution.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkPipelineMetrics implements PipelineMetrics {

  private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineMetrics.class);

  private static ConcurrentHashMap<String, JobClient> jobClientMap = new ConcurrentHashMap<>();

  private static CumulativeMetrics cumulativeMetrics = new CumulativeMetrics(0l, 0l, 0l);

  @Override
  public synchronized CumulativeMetrics getCumulativeMetricsForOngoingBatch() {
    List<MetricQueryResults> onGoingMetricQueryResults = getOngoingMetricQueryResults();
    return getUpdatedCumulativeMetrics(cumulativeMetrics, onGoingMetricQueryResults);
  }

  @Override
  public synchronized void clearAllMetrics() {
    jobClientMap.clear();
    cumulativeMetrics = new CumulativeMetrics(0l, 0l, 0l);
  }

  @Override
  public synchronized void setTotalNoOfResources(long totalNoOfResources) {
    cumulativeMetrics = new CumulativeMetrics(totalNoOfResources, 0l, 0l);
  }

  /**
   * This method returns the MetricQueryResults for the currently running pipeline. The current
   * running pipeline is determined by the JobClient which is set by the FlinkJobListener when the
   * pipeline is started.
   *
   * @return MetricQueryResults
   */
  private List<MetricQueryResults> getOngoingMetricQueryResults() {
    List<MetricQueryResults> metricQueryResultsList = new ArrayList<>();
    if (jobClientMap == null || jobClientMap.isEmpty()) {
      return metricQueryResultsList;
    }

    for (JobClient jobClient : jobClientMap.values()) {
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
          MetricQueryResults metricQueryResults =
              metricResults.queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(
                          MetricNameFilter.inNamespace(MetricsConstants.METRICS_NAMESPACE))
                      .build());
          metricQueryResultsList.add(metricQueryResults);
        }
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Caught an exception; interrupting! ", e);
        Thread.currentThread().interrupt();
      }
    }
    return metricQueryResultsList;
  }

  private static CumulativeMetrics getUpdatedCumulativeMetrics(
      CumulativeMetrics currentMetrics, List<MetricQueryResults> metricQueryResultsList) {
    if (metricQueryResultsList == null || metricQueryResultsList.isEmpty()) {
      return cumulativeMetrics;
    }
    long totalNoOfFetchedResources = 0l;
    long totalNoOfMappedResources = 0l;
    for (MetricQueryResults metricQueryResults : metricQueryResultsList) {
      for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
        if (counter.getName().getName().startsWith(MetricsConstants.NUM_FETCHED_RESOURCES)) {
          totalNoOfFetchedResources += counter.getAttempted();
        }
        if (counter.getName().getName().startsWith(MetricsConstants.NUM_MAPPED_RESOURCES)) {
          totalNoOfMappedResources += counter.getAttempted();
        }
      }
    }
    return new CumulativeMetrics(
        currentMetrics.getTotalResources(),
        currentMetrics.getFetchedResources() + totalNoOfFetchedResources,
        currentMetrics.getMappedResources() + totalNoOfMappedResources);
  }

  private static MetricQueryResults getMetricQueryResults(JobClient jobClient) {
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

  public static synchronized void addJobClient(JobClient jobClient) {
    jobClientMap.put(jobClient.getJobID().toHexString(), jobClient);
  }

  public static synchronized void removeJobClient(String jobClientId) {
    JobClient jobClient = jobClientMap.remove(jobClientId);
    if (jobClient != null) {
      MetricQueryResults metricQueryResults = getMetricQueryResults(jobClient);
      CumulativeMetrics updatedMetrics =
          getUpdatedCumulativeMetrics(cumulativeMetrics, Arrays.asList(metricQueryResults));
      cumulativeMetrics = updatedMetrics;
    }
  }

  private static MetricQueryResults getEmptyMetricQueryResults() {
    return MetricQueryResults.create(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
  }
}
