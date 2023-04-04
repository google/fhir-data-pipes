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

import javax.annotation.Nullable;
import lombok.Data;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.openmrs.analytics.MetricsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class Stats {

  private Integer percentageCompleted = 0;

  private static final Logger logger = LoggerFactory.getLogger(Stats.class.getName());
  private static final Integer MAPPED_RESOURCES_WEIGHT = 4;
  private static final Integer PARSED_RESOURCES_WEIGHT = 1;

  /**
   * This method is used to convert the MetricQueryResults into Stats. If the metricQueryResults is
   * null, then an empty stats object is returned. Percentage completed is the stats that is
   * currently generated, this is calculated using the ratio (Total resources processed so far /
   * Total number of resources to be processed). There are multiple steps involved in processing the
   * records. The completion percentage is derived as a weighted average of these steps. The
   * weightage for each step is a rough approximation derived based on the time taken for that step
   * as compared to the other steps.
   *
   * @param metricQueryResults
   * @return Stats
   */
  public static Stats createStats(@Nullable MetricQueryResults metricQueryResults) {

    if (metricQueryResults == null) {
      return null;
    }

    Stats stats = new Stats();
    long totalNoOfResources = 0l;
    for (MetricResult<GaugeResult> gauge : metricQueryResults.getGauges()) {
      if (gauge.getName().getName().startsWith(MetricsConstants.TOTAL_NO_OF_RESOURCES)) {
        totalNoOfResources += gauge.getAttempted().getValue();
      }
    }

    long totalNoOfFetchedResources = 0l;
    long totalNoOfMappedResources = 0l;
    for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
      if (counter.getName().getName().startsWith(MetricsConstants.NUM_FETCHED_RESOURCES)) {
        totalNoOfFetchedResources += counter.getAttempted();
      }
      if (counter.getName().getName().startsWith(MetricsConstants.NUM_MAPPED_RESOURCES)) {
        totalNoOfMappedResources += counter.getAttempted();
      }
    }

    int mappedResourcesPercentage =
        getPercentage(Math.min(totalNoOfMappedResources, totalNoOfResources), totalNoOfResources);
    int fetchedResourcesPercentage =
        getPercentage(Math.min(totalNoOfFetchedResources, totalNoOfResources), totalNoOfResources);

    // Each step might differ in the time it takes to complete. So, weights are assigned for each
    // step which are derived approximately based on the time taken for that step as compared to the
    // other steps
    int weightedAveragePercentage =
        ((MAPPED_RESOURCES_WEIGHT * mappedResourcesPercentage)
                + (PARSED_RESOURCES_WEIGHT * fetchedResourcesPercentage))
            / (MAPPED_RESOURCES_WEIGHT + PARSED_RESOURCES_WEIGHT);
    stats.setPercentageCompleted(weightedAveragePercentage);
    return stats;
  }

  private static Integer getPercentage(long actual, long total) {
    double percentage = 0;
    if (total > 0) {
      long numerator = Math.min(actual, total);
      percentage = ((double) numerator / (double) total) * 100;
    }
    return (int) Math.ceil(percentage);
  }
}
