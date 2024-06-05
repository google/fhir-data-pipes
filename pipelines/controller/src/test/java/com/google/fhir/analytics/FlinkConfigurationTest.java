/*
 * Copyright 2020-2024 Google LLC
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

import static com.google.fhir.analytics.FlinkConfiguration.DEFAULT_MANAGED_MEMORY_SIZE;
import static com.google.fhir.analytics.FlinkConfiguration.NUMBER_OF_RESHUFFLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FlinkConfigurationTest {

  @ParameterizedTest
  @MethodSource("provideFlinkParams")
  public void testConfigurations(int numThreads, long expectedNetworkMemory) throws IOException {
    DataProperties dataProperties = new DataProperties();
    dataProperties.setAutoGenerateFlinkConfiguration(true);
    dataProperties.setNumThreads(numThreads);
    FlinkConfiguration flinkConfiguration = new FlinkConfiguration();
    flinkConfiguration.initialiseFlinkConfiguration(dataProperties);

    assertThat(
        Files.exists(
            Paths.get(
                flinkConfiguration.getFlinkConfDir(), GlobalConfiguration.FLINK_CONF_FILENAME)),
        equalTo(Boolean.TRUE));
    Configuration configuration =
        GlobalConfiguration.loadConfiguration(flinkConfiguration.getFlinkConfDir());
    assertThat(
        configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX).getBytes(),
        equalTo(expectedNetworkMemory));
    assertThat(
        configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE).getBytes(),
        equalTo(MemorySize.parseBytes(DEFAULT_MANAGED_MEMORY_SIZE)));
  }

  private static Stream<Arguments> provideFlinkParams() {
    return Stream.of(
        // The percentage completion is rounded off to next highest integer
        Arguments.of(2, getNetworkMemory(2)), Arguments.of(4, getNetworkMemory(4)));
  }

  private static long getNetworkMemory(int numThreads) {
    return (int) (Math.pow((numThreads + 1), 2))
        * NUMBER_OF_RESHUFFLES
        * TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().getBytes();
  }

  @Test
  public void testNonAutoGenerationWithDefaultConfiguration() throws IOException {
    DataProperties dataProperties = new DataProperties();
    dataProperties.setAutoGenerateFlinkConfiguration(false);
    dataProperties.setNumThreads(1);
    try (MockedStatic<GlobalConfiguration> mockedStatic =
        Mockito.mockStatic(GlobalConfiguration.class)) {
      Configuration defaultConfiguration = new Configuration();
      mockedStatic
          .when(() -> GlobalConfiguration.loadConfiguration())
          .thenReturn(defaultConfiguration);
      FlinkConfiguration flinkConfiguration = new FlinkConfiguration();

      flinkConfiguration.initialiseFlinkConfiguration(dataProperties);

      mockedStatic.verify(() -> GlobalConfiguration.loadConfiguration(), Mockito.times(1));
    }
  }
}
