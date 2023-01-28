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

import java.io.IOException;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EtlUtils {

  static final String METRICS_NAMESPACE = "PipelineMetrics";

  private static final Logger log = LoggerFactory.getLogger(EtlUtils.class);

  static MetricQueryResults getMetrics(MetricResults metricResults) {
    return metricResults.queryMetrics(
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(METRICS_NAMESPACE))
            .build());
  }

  static void logMetrics(MetricResults metricResults) {
    MetricQueryResults metrics = getMetrics(metricResults);
    for (MetricResult<Long> counter : metrics.getCounters()) {
      log.info(
          String.format("Pipeline counter %s : %s", counter.getName(), counter.getAttempted()));
    }
  }

  /**
   * Runs the given `pipeline` and if the output is to a Parquet DWH, also writes a timestamp file
   * indicating when the pipeline was started. This is useful for future incremental runs.
   *
   * @return the result from running the pipeline.
   * @throws IOException if writing the timestamp file fails.
   */
  static PipelineResult runPipelineWithTimestamp(Pipeline pipeline, FhirEtlOptions options)
      throws IOException {
    String dwhRoot = options.getOutputParquetPath();
    if (dwhRoot != null && !dwhRoot.isEmpty()) {
      // TODO write pipeline options too such that it  can be validated for incremental runs.
      DwhFiles.forRoot(dwhRoot).writeTimestampFile();
    }
    return runPipeline(pipeline);
  }

  /** Similar to {@link #runPipelineWithTimestamp} but for the merge pipeline. */
  static PipelineResult runMergerPipelineWithTimestamp(
      Pipeline pipeline, ParquetMergerOptions options) throws IOException {
    Instant instant1 = DwhFiles.forRoot(options.getDwh1()).readTimestampFile();
    Instant instant2 = DwhFiles.forRoot(options.getDwh2()).readTimestampFile();
    Instant mergedInstant = (instant1.compareTo(instant2) > 0) ? instant1 : instant2;
    DwhFiles.forRoot(options.getMergedDwh()).writeTimestampFile(mergedInstant);
    return runPipeline(pipeline);
  }

  private static PipelineResult runPipeline(Pipeline pipeline) {
    // Note that with even with FlinkRunner, in the "local" mode the next call is blocking.
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    EtlUtils.logMetrics(result.metrics());
    return result;
  }
}
