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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import ca.uhn.fhir.context.FhirVersionEnum;
import com.google.common.base.Preconditions;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.fhir.analytics.model.BulkExportResponse.Output;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkExportUtilTest {
  // Initialization handled by Mockito's @Mock annotation
  @SuppressWarnings("NullAway.Init")
  @Mock
  private BulkExportApiClient bulkExportApiClient;

  private BulkExportUtil bulkExportUtil;

  @Before
  public void setup() throws IOException {
    Preconditions.checkNotNull(bulkExportApiClient);
    bulkExportUtil = new BulkExportUtil(bulkExportApiClient);
  }

  @Test(timeout = 10000)
  public void testTriggerBulkExport() throws IOException {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    String since = "2021-01-01T00:00:00Z";
    String mockLocationUrl =
        "http://localhost:8080/fhir/$export-poll-status?_jobId=2961c268-027e-49cb-839c-4c1ef76615c6";
    Mockito.when(bulkExportApiClient.triggerBulkExportJob(resourceTypes, since, FhirVersionEnum.R4))
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

    BulkExportResponse actualBulkExportResponse =
        bulkExportUtil.triggerBulkExport(resourceTypes, since, FhirVersionEnum.R4);
    assertThat(actualBulkExportResponse.output().size(), equalTo(2));
    assertThat(
        actualBulkExportResponse.output(),
        containsInAnyOrder(
            Output.builder().type("Patient").url("http://localhost:8080/file1").build(),
            Output.builder().type("Observation").url("http://localhost:8080/file2").build()));
    Mockito.verify(bulkExportApiClient, Mockito.times(1))
        .triggerBulkExportJob(resourceTypes, since, FhirVersionEnum.R4);
    Mockito.verify(bulkExportApiClient, Mockito.times(2))
        .fetchBulkExportHttpResponse(mockLocationUrl);
  }
}
