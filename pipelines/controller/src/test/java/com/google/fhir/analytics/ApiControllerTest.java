/*
 * Copyright 2020-2025 Google LLC
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

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ApiController.class)
public class ApiControllerTest {
  @Autowired private MockMvc mockMvc;

  @MockitoBean private PipelineManager pipelineManager;

  @MockitoBean private DataProperties dataProperties;

  @MockitoBean private DwhFilesManager dwhFilesManager;

  @Test
  public void getNextScheduled() throws Exception {
    when(pipelineManager.getNextIncrementalTime()).thenReturn(LocalDateTime.of(2025, 8, 6, 11, 00));
    mockMvc
        .perform(get("/next"))
        .andExpect(status().is2xxSuccessful())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json("{\"next_run\":\"2025-08-06T11:00\"}"));
  }

  @Test
  public void getNextScheduledForNone() throws Exception {
    mockMvc
        .perform(get("/next"))
        .andExpect(status().is2xxSuccessful())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json("{\"next_run\":\"NOT SCHEDULED\"}"));
  }

  @Test
  public void testGetDwh() throws Exception {
    when(dataProperties.getDwhRootPrefix()).thenReturn("dwh/controller_DEV_DWH");
    when(pipelineManager.getCurrentDwhRoot())
        .thenReturn("/path/to/dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z");
    when(dwhFilesManager.listDwhSnapshots())
        .thenReturn(
            List.of(
                "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z",
                "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677808Z"));
    String expectedJson =
        """
                {
                    "dwh_prefix": "dwh/controller_DEV_DWH",
                    "dwh_path_latest": "/path/to/dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z",
                    "dwh_snapshot_ids": [
                        "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z",
                        "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677808Z"
                    ]
                }
                """;
    mockMvc
        .perform(get("/dwh"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json(expectedJson));
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    doNothing()
        .when(dwhFilesManager)
        .deleteDwhSnapshotFiles("dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z");
    mockMvc
        .perform(
            delete("/dwh?snapshotId=dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z"))
        .andExpect(status().isNoContent());
    verify(dwhFilesManager, times(1))
        .deleteDwhSnapshotFiles("dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z");
  }

  @Test
  public void testDeleteSnapshotForNoSnapshotId() throws Exception {
    doNothing()
        .when(dwhFilesManager)
        .deleteDwhSnapshotFiles("dwh/controller_DEV_DWH_TIMESTAMP_2025_08_06T16_33_07_677807Z");
    mockMvc.perform(delete("/dwh?noSnapshotId=")).andExpect(status().isBadRequest());
  }

  @Test
  public void testGetConfigs() throws Exception {
    DataProperties.ConfigFields configField1 =
        new DataProperties.ConfigFields(
            "test.config.uno", "testValueUno", "Test Description Uno", "defaultTestValueUno");
    DataProperties.ConfigFields configField2 =
        new DataProperties.ConfigFields(
            "test.config.dos", "testValueDos", "Test Description Dos", "defaultTestValueDos");
    when(dataProperties.getConfigParams()).thenReturn(List.of(configField1, configField2));
    String expectedJson =
        """
                {
                    "test.config.uno": "testValueUno",
                    "test.config.dos": "testValueDos"
                }
                """;
    mockMvc
        .perform(get("/config"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json(expectedJson));
  }

  @Test
  public void testGetConfigsForConfigField() throws Exception {
    DataProperties.ConfigFields configField1 =
        new DataProperties.ConfigFields(
            "test.config.uno", "testValueUno", "Test Description Uno", "defaultTestValueUno");
    DataProperties.ConfigFields configField2 =
        new DataProperties.ConfigFields(
            "test.config.dos", "testValueDos", "Test Description Dos", "defaultTestValueDos");
    when(dataProperties.getConfigParams()).thenReturn(List.of(configField1, configField2));
    String expectedJson =
        """
                {
                    "test.config.uno": "testValueUno"
                }
                """;
    mockMvc
        .perform(get("/config/test.config.uno"))
        .andExpect(status().isOk())
        .andExpect(content().contentType(APPLICATION_JSON))
        .andExpect(content().json(expectedJson));
  }
}
