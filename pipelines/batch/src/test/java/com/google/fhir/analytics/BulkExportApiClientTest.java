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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.apache.ApacheHttpResponse;
import ca.uhn.fhir.rest.client.api.IHttpResponse;
import ca.uhn.fhir.util.StopWatch;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.gson.Gson;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkExportApiClientTest {

  @Mock private FetchUtil fetchUtil;

  private BulkExportApiClient bulkExportApiClient;

  @Before
  public void setup() throws IOException {
    bulkExportApiClient = new BulkExportApiClient(fetchUtil);
  }

  @Test
  public void testTriggerBulkExportJob() {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    String mockLocationUrl =
        "http://localhost:8080/fhir/$export-poll-status?_jobId=2961c268-027e-49cb-839c-4c1ef76615c6";
    MethodOutcome methodOutcome = new MethodOutcome();
    methodOutcome.setResponseStatusCode(HttpStatus.SC_ACCEPTED);
    Map<String, List<String>> outputHeaders = Maps.newHashMap();
    outputHeaders.put("content-location", Arrays.asList(mockLocationUrl));
    methodOutcome.setResponseHeaders(outputHeaders);
    when(fetchUtil.performServerOperation(any(), any(), any())).thenReturn(methodOutcome);

    String contentLocationUrl =
        bulkExportApiClient.triggerBulkExportJob(resourceTypes, null, FhirVersionEnum.R4);

    assertThat(contentLocationUrl, equalTo(mockLocationUrl));
    Mockito.verify(fetchUtil, times(1)).performServerOperation(any(), any(), any());
  }

  @Test(expected = IllegalStateException.class)
  public void testTriggerBulkExportJobError() {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    MethodOutcome methodOutcome = new MethodOutcome();
    methodOutcome.setResponseStatusCode(HttpStatus.SC_BAD_REQUEST);
    when(fetchUtil.performServerOperation(any(), any(), any())).thenReturn(methodOutcome);

    bulkExportApiClient.triggerBulkExportJob(resourceTypes, null, FhirVersionEnum.R4);
  }

  @Test
  public void testFetchBulkExportHttpResponse() throws IOException {
    StopWatch responseStopWatch = new StopWatch();
    HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK");
    response.addHeader("expires", "Mon, 22 Jul 2019 23:59:59 GMT");
    response.addHeader("retry-after", "120");
    URL url = Resources.getResource("bulk_export_response.json");
    String bulkResponseString = Resources.toString(url, StandardCharsets.UTF_8);
    response.setEntity(new StringEntity(bulkResponseString));
    IHttpResponse expectedResponse = new ApacheHttpResponse(response, responseStopWatch);
    String mockLocationUrl =
        "http://localhost:8080/fhir/$export-poll-status?_jobId=2961c268-027e-49cb-839c-4c1ef76615c6";
    when(fetchUtil.fetchResponseForUrl(mockLocationUrl)).thenReturn(expectedResponse);

    BulkExportHttpResponse bulkExportHttpResponse =
        bulkExportApiClient.fetchBulkExportHttpResponse(mockLocationUrl);

    assertThat(bulkExportHttpResponse.httpStatus(), equalTo(HttpStatus.SC_OK));
    assertThat(bulkExportHttpResponse.retryAfter(), equalTo(120));
    assertThat(
        bulkExportHttpResponse.expires(), equalTo(new Date("Mon, 22 Jul 2019 23:59:59 GMT")));
    Gson gson = new Gson();
    assertThat(
        bulkExportHttpResponse.bulkExportResponse(),
        equalTo(gson.fromJson(bulkResponseString, BulkExportResponse.class)));
  }
}
