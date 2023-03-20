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

import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.openmrs.analytics.MetricsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsHelper {
  private static final Logger logger = LoggerFactory.getLogger(StatsHelper.class.getName());

  public static Stats createStats(MetricQueryResults metricQueryResults) {

    Stats stats = new Stats();
    if (metricQueryResults == null) return stats;

    long totalNoOfResources = 0l;
    for (MetricResult<GaugeResult> gauge : metricQueryResults.getGauges()) {
      if (gauge.getName().getName().startsWith(MetricsConstants.TOTAL_NO_OF_RESOURCES)) {
        totalNoOfResources += gauge.getAttempted().getValue();
      }
    }

    long totalNoOfFetchedResources = 0l;
    for (MetricResult<Long> counter : metricQueryResults.getCounters()) {
      if (counter.getName().getName().startsWith(MetricsConstants.NUM_FETCHED_RESOURCES)) {
        totalNoOfFetchedResources += counter.getAttempted();
      }
    }
    double percentageCompletedDouble = 0;
    if (totalNoOfResources > 0) {
      percentageCompletedDouble =
          ((double) totalNoOfFetchedResources / (double) totalNoOfResources) * 100;
    }
    stats.setPercentageCompleted((int) percentageCompletedDouble);
    return stats;
  }
}
