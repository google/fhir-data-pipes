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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.fhir.analytics.MetricsConstants;
import java.util.stream.Stream;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class StatsTest {

  private static Stream<Arguments> provideMetricsForStats() {
    return Stream.of(
        // The percentage completion is rounded off to next highest integer
        Arguments.of(1L, 1L, 2000000L, 1),
        Arguments.of(3000L, 3000L, 40000L, 8),
        // Percentage completion is the weighted average of the noOfFetchedResources and
        // noOfMappedResources with different weights
        Arguments.of(500L, 250L, 1000L, 30),
        Arguments.of(250L, 500L, 1000L, 45),
        Arguments.of(5000L, 5000L, 10000L, 50),
        Arguments.of(30000L, 30000L, 30000L, 100));
  }

  @ParameterizedTest
  @MethodSource("provideMetricsForStats")
  public void createStatsTest(
      Long noOfFetchedResources,
      Long noOfMappedResources,
      Long totalNoOfResources,
      Integer expectedPercentageCompletion) {
    ImmutableList.Builder<MetricResult<Long>> counterResults = ImmutableList.builder();
    counterResults.add(
        MetricResult.attempted(
            MetricKey.create(
                "Dummy Step1",
                MetricName.named(
                    MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_FETCHED_RESOURCES)),
            noOfFetchedResources));
    counterResults.add(
        MetricResult.attempted(
            MetricKey.create(
                "Dummy Step2",
                MetricName.named(
                    MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_MAPPED_RESOURCES)),
            noOfMappedResources));
    ImmutableList.Builder<MetricResult<GaugeResult>> gaugeResults = ImmutableList.builder();
    gaugeResults.add(
        MetricResult.attempted(
            MetricKey.create(
                "Dummy Step1",
                MetricName.named(
                    MetricsConstants.METRICS_NAMESPACE, MetricsConstants.TOTAL_NO_OF_RESOURCES)),
            GaugeResult.create(totalNoOfResources, Instant.now())));
    ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults =
        ImmutableList.builder();
    MetricQueryResults metricQueryResults =
        MetricQueryResults.create(
            counterResults.build(), distributionResults.build(), gaugeResults.build());

    Stats stats = Stats.createStats(metricQueryResults);

    assertThat(stats.getPercentageCompleted(), equalTo(expectedPercentageCompletion));
  }
}
