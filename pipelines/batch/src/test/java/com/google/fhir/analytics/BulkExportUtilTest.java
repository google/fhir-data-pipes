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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.fhir.analytics.model.BulkExportResponse.Output;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkExportUtilTest {

  @Mock private BulkExportApiClient bulkExportApiClient;

  private BulkExportUtil bulkExportUtil;

  @Before
  public void setup() throws IOException {
    bulkExportUtil = new BulkExportUtil(bulkExportApiClient);
  }

  @Test(timeout = 10000)
  public void testTriggerBulkExport() throws IOException {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    String mockLocationUrl =
        "http://localhost:8080/fhir/$export-poll-status?_jobId=2961c268-027e-49cb-839c-4c1ef76615c6";
    Mockito.when(bulkExportApiClient.triggerBulkExportJob(resourceTypes, FhirVersionEnum.R4))
        .thenReturn(mockLocationUrl);

    BulkExportHttpResponse acceptedResponse =
        BulkExportHttpResponse.builder()
            .httpStatus(HttpStatus.SC_ACCEPTED)
            .retryAfter(5) // Retry after 5 secs
            .build();

    BulkExportResponse bulkExportResponse =
        BulkExportResponse.builder()
            .output(
                Arrays.asList(
                    Output.builder().type("Patient").url("http://localhost:8080/file1").build(),
                    Output.builder()
                        .type("Observation")
                        .url("http://localhost:8080/file2")
                        .build()))
            .build();
    BulkExportHttpResponse completedResponse =
        BulkExportHttpResponse.builder()
            .httpStatus(HttpStatus.SC_OK)
            .bulkExportResponse(bulkExportResponse)
            .build();
    Mockito.when(bulkExportApiClient.fetchBulkExportHttpResponse(mockLocationUrl))
        .thenReturn(acceptedResponse, completedResponse);

    Map<String, List<String>> typeToFileMapping =
        bulkExportUtil.triggerBulkExport(resourceTypes, FhirVersionEnum.R4);
    assertThat(typeToFileMapping.size(), equalTo(2));
    assertThat(
        typeToFileMapping.get("Patient"), equalTo(Arrays.asList("http://localhost:8080/file1")));
    assertThat(
        typeToFileMapping.get("Observation"),
        equalTo(Arrays.asList("http://localhost:8080/file2")));

    Mockito.verify(bulkExportApiClient, Mockito.times(1))
        .triggerBulkExportJob(resourceTypes, FhirVersionEnum.R4);
    Mockito.verify(bulkExportApiClient, Mockito.times(2))
        .fetchBulkExportHttpResponse(mockLocationUrl);
  }
}
