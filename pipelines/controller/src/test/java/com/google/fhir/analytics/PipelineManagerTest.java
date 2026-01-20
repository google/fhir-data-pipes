/*
 * Copyright 2020-2026 Google LLC
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.support.CronExpression;

@SuppressWarnings("NullAway")
@ExtendWith(MockitoExtension.class)
public class PipelineManagerTest {

  private PipelineManager pipelineManager;

  private final LocalDateTime lastRunEndTimestamp = LocalDateTime.of(2025, 12, 29, 10, 0);

  @BeforeEach
  void setUp() throws Exception {
    DataProperties dataProperties = mock(DataProperties.class);
    DwhFilesManager dwhFilesManager = mock(DwhFilesManager.class);
    MeterRegistry meterRegistry = mock(MeterRegistry.class);
    pipelineManager = new PipelineManager();
    setField("dataProperties", dataProperties);
    setField("dwhFilesManager", dwhFilesManager);
    setField("meterRegistry", meterRegistry);
    setField("cron", CronExpression.parse("0 * * * * *")); // every minute
    setField("lastRunEnd", lastRunEndTimestamp);
    setField("currentPipeline", null); // not running
  }

  private void setField(String fieldName, Object value) throws Exception {
    Field field = PipelineManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(pipelineManager, value);
  }

  @Test
  public void testIncrementalModeTriggeredAtRightTime() throws Exception {
    // Mock current time to be after next scheduled time
    LocalDateTime currentTime = lastRunEndTimestamp.plusMinutes(2); // 2 minutes after lastRunEnd
    try (MockedStatic<DwhFilesManager> mockedDwh = mockStatic(DwhFilesManager.class)) {
      mockedDwh.when(DwhFilesManager::getCurrentTime).thenReturn(currentTime);

      IllegalStateException illegalStateException =
          assertThrows(
              IllegalStateException.class,
              () -> {
                // We have wrapped in assertThrows because runIncrementalPipeline throws due to
                // unmocked
                // dependencies
                pipelineManager.checkSchedule();

                // The incremental pipeline should be triggered since current time is after next
                // time
                // Note: In a real scenario, currentPipeline would be set, but in test,
                // runIncrementalPipeline
                // will fail due to unmocked dependencies
                // The log message "Incremental run triggered" indicates the triggering logic worked
                // For this test, we assert that the exception message is as expected. We can only
                // get that message if the pipeline was triggered, i.e. runIncrementalPipeline() was
                // invoked.

              });

      assertThat(
          illegalStateException.getMessage(),
          equalTo(
              "cannot start the incremental pipeline while there are no DWHs; run full pipeline"));
    }
  }

  @Test
  public void testIncrementalModeNotTriggeredBeforeTime() throws Exception {
    // Mock current time to be before next scheduled time
    LocalDateTime currentTime =
        LocalDateTime.of(2025, 12, 29, 10, 0, 30); // 30 seconds after, but cron is every minute
    try (MockedStatic<DwhFilesManager> mockedDwh = mockStatic(DwhFilesManager.class)) {
      mockedDwh.when(DwhFilesManager::getCurrentTime).thenReturn(currentTime);

      pipelineManager.checkSchedule();

      // The incremental pipeline should not be triggered since current time is before next time
      // Assert that currentPipeline is not running
      assertThat(pipelineManager.isRunning(), is(false));
    }
  }

  @Test
  public void testGetNextIncrementalTime() {
    LocalDateTime next = pipelineManager.getNextIncrementalTime();
    // Since lastRunEnd is 10:00, next should be 10:01
    assertThat(next, is(equalTo(LocalDateTime.of(2025, 12, 29, 10, 1))));
  }

  @Test
  public void testGetNextIncrementalTimeWhenNoPreviousRun() throws Exception {
    // Set lastRunEnd to null to simulate no previous run
    setField("lastRunEnd", (LocalDateTime) null);
    LocalDateTime next = pipelineManager.getNextIncrementalTime();
    assertThat(next, is(nullValue()));
  }
}
