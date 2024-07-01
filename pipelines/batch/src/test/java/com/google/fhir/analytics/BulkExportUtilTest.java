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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.fhir.analytics.exception.BulkExportException;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.fhir.analytics.model.BulkExportResponse.Output;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkExportUtilTest {

  @Mock private ExecutorService executorService;
  @Mock private FhirSearchUtil fhirSearchUtil;

  @Test
  public void testTriggerBulkExportAndDownloadFiles() throws BulkExportException, IOException {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    FhirVersionEnum fhirVersionEnum = FhirVersionEnum.R4;

    String mockLocationUrl =
        "http://localhost:8080/fhir/$export-poll-status?_jobId=2961c268-027e-49cb-839c-4c1ef76615c6";
    Mockito.when(fhirSearchUtil.triggerBulkExportJob(resourceTypes, fhirVersionEnum))
        .thenReturn(mockLocationUrl);
    BulkExportHttpResponse bulkExportHttpResponse = new BulkExportHttpResponse();
    bulkExportHttpResponse.setHttpStatus(HttpStatus.SC_OK);
    BulkExportResponse bulkExportResponse = new BulkExportResponse();
    List<Output> outputList = new ArrayList<>();
    Output output1 = new Output();
    output1.setType("Patient");
    output1.setUrl("http://localhost:8080/fhir/16886hjsdhjshdjsj");
    outputList.add(output1);
    Output output2 = new Output();
    output2.setType("Observation");
    output2.setUrl("http://localhost:8080/fhir/dsdsdaasaXSDCSDSC");
    outputList.add(output2);
    bulkExportResponse.setOutput(outputList);
    bulkExportHttpResponse.setBulkExportResponse(bulkExportResponse);

    Path tempDir = Path.of(BulkExportUtil.TEMP_BULK_EXPORT_PATH_PREFIX);
    Mockito.when(fhirSearchUtil.fetchBulkExportHttpResponse(mockLocationUrl))
        .thenReturn(bulkExportHttpResponse);
    try (MockedStatic<Executors> executorsMockedStatic = Mockito.mockStatic(Executors.class);
        MockedStatic<Files> filesMockedStatic = Mockito.mockStatic(Files.class)) {
      executorsMockedStatic
          .when(() -> Executors.newFixedThreadPool(BulkExportUtil.NUM_OF_PARALLEL_DOWNLOADS))
          .thenReturn(executorService);

      filesMockedStatic
          .when(() -> Files.createTempDirectory(BulkExportUtil.TEMP_BULK_EXPORT_PATH_PREFIX))
          .thenReturn(tempDir);

      Mockito.when(executorService.submit(Mockito.any(Runnable.class)))
          .thenReturn(CompletableFuture.completedFuture(null));

      List<Path> ndJsonDirectoryPaths =
          BulkExportUtil.triggerBulkExportAndDownloadFiles(
              resourceTypes, fhirVersionEnum, fhirSearchUtil);

      MatcherAssert.assertThat(
          ndJsonDirectoryPaths.toArray(),
          Matchers.arrayContainingInAnyOrder(
              Path.of(BulkExportUtil.TEMP_BULK_EXPORT_PATH_PREFIX, "Patient"),
              Path.of(BulkExportUtil.TEMP_BULK_EXPORT_PATH_PREFIX, "Observation")));
      executorsMockedStatic.verify(
          () -> Executors.newFixedThreadPool(BulkExportUtil.NUM_OF_PARALLEL_DOWNLOADS),
          Mockito.times(1));
      filesMockedStatic.verify(
          () -> Files.createTempDirectory(BulkExportUtil.TEMP_BULK_EXPORT_PATH_PREFIX),
          Mockito.times(1));
      Mockito.verify(executorService, Mockito.times(2)).submit(Mockito.any(Runnable.class));
    } finally {
      FileUtils.deleteDirectory(tempDir.toFile());
    }
  }
}
