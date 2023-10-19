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

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StatsTest {

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
  void createStatsTest(
      Long noOfFetchedResources,
      Long noOfMappedResources,
      Long totalNoOfResources,
      Integer expectedPercentageCompletion) {
    CumulativeMetrics cumulativeMetrics =
        new CumulativeMetrics(totalNoOfResources, noOfFetchedResources, noOfMappedResources);
    Stats stats = Stats.createStats(cumulativeMetrics);
    assertThat(stats.getPercentageCompleted(), equalTo(expectedPercentageCompletion));
  }
}
