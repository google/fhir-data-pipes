/*
 * Copyright 2020-2022 Google LLC
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

import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EtlUtils {

  static final String METRICS_NAMESPACE = "FhirEtl";

  private static final Logger log = LoggerFactory.getLogger(EtlUtils.class);

  static void logMetrics(MetricResults metricResults) {
    MetricQueryResults metrics =
        metricResults.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.inNamespace(METRICS_NAMESPACE))
                .build());
    for (MetricResult<Long> counter : metrics.getCounters()) {
      log.info(
          String.format("Pipeline counter %s : %s", counter.getName(), counter.getCommitted()));
    }
  }
}
