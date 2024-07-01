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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SearchTotalModeEnum;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.apache.ApacheHttpResponse;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IHttpResponse;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.util.StopWatch;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.google.fhir.analytics.exception.BulkExportException;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.hl7.fhir.r4.model.Bundle;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirSearchUtilTest {

  private static final String PAGE_URL_PARAM = "_getpages=861af9b4-d847-4831-b945-ab1f6f08f03e";

  private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R4";

  private static final String SEARCH_URL = "Patient?given=TEST";

  @Mock private FetchUtil fetchUtil;

  @Mock private IGenericClient genericClient;

  @Mock private IUntypedQuery untypedQuery;

  @Mock private IQuery query;

  private Bundle bundle;

  private static FhirContext fhirContext;

  private FhirSearchUtil fhirSearchUtil;

  @BeforeClass
  public static void setupFhirContext() {
    fhirContext = FhirContext.forR4Cached();
  }

  @Before
  public void setup() throws IOException {
    URL url = Resources.getResource("bundle.json");
    String bundleStr = Resources.toString(url, StandardCharsets.UTF_8);
    IParser parser = fhirContext.newJsonParser();
    bundle = parser.parseResource(Bundle.class, bundleStr);
    fhirSearchUtil = new FhirSearchUtil(fetchUtil);
    when(fetchUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
    when(fetchUtil.getSourceClient()).thenReturn(genericClient);
    when(fetchUtil.getSourceClient(anyBoolean())).thenReturn(genericClient);
    when(genericClient.search()).thenReturn(untypedQuery);
    when(untypedQuery.byUrl(SEARCH_URL)).thenReturn(query);
    when(untypedQuery.forResource(anyString())).thenReturn(query);
    when(query.count(anyInt())).thenReturn(query);
    when(query.totalMode(any(SearchTotalModeEnum.class))).thenReturn(query);
    when(query.summaryMode(any(SummaryEnum.class))).thenReturn(query);
    when(query.returnBundle(any())).thenReturn(query);
    when(query.execute()).thenReturn(bundle);
  }

  @Test
  public void testFindBaseSearchUrl() {
    String baseUrl = fhirSearchUtil.findBaseSearchUrl(bundle);
    assertThat(baseUrl, equalTo(BASE_URL + "?" + PAGE_URL_PARAM));
  }

  @Test
  public void testSearchForResource() {
    Bundle actualBundle = fhirSearchUtil.searchByUrl(SEARCH_URL, 10, SummaryEnum.DATA);
    assertThat(actualBundle.equalsDeep(bundle), equalTo(true));
  }

  @Test
  public void testGetNextUrlNull() {
    String bundleStr =
        "{ \"resourceType\": \"Bundle\",\n"
            + "    \"id\": \"3beab86b-ca1a-427b-8c5f-07635010c1d5\",\n"
            + "    \"meta\": { \"lastUpdated\": \"2021-04-22T05:08:33.750+03:00\" },\n"
            + "    \"type\": \"searchset\",\n"
            + "    \"total\": 0,\n"
            + "    \"link\": [\n"
            + "        {\n"
            + "            \"relation\": \"self\",\n"
            + "            \"url\":"
            + " \"http://fhir_server/Encounter?_count=100&_summary=data\"\n"
            + "        }\n"
            + "    ]\n"
            + "}";
    IParser parser = fhirContext.newJsonParser();
    bundle = parser.parseResource(Bundle.class, bundleStr);
    String nextUrl = fhirSearchUtil.getNextUrl(bundle);
    assertThat(nextUrl, nullValue());
  }

  @Test
  public void testCreateSegments() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setResourceList("Patient");
    options.setBatchSize(15);
    Map<String, List<SearchSegmentDescriptor>> segmentMap = fhirSearchUtil.createSegments(options);
    assertThat(segmentMap.size(), equalTo(1));
    assertThat(segmentMap.get("Patient").size(), equalTo(4));
  }

  @Test
  public void testCreateSegmentsWithSince() {
    FhirEtlOptions options = PipelineOptionsFactory.as(FhirEtlOptions.class);
    options.setResourceList("Patient");
    options.setBatchSize(15);
    options.setSince("2020-12-01");
    when(query.lastUpdated(any())).thenReturn(query);
    Map<String, List<SearchSegmentDescriptor>> segmentMap = fhirSearchUtil.createSegments(options);
    assertThat(segmentMap.size(), equalTo(1));
    assertThat(segmentMap.get("Patient").size(), equalTo(4));
    ArgumentCaptor<DateRangeParam> dateCaptor = ArgumentCaptor.forClass(DateRangeParam.class);
    verify(query).lastUpdated(dateCaptor.capture());
    DateRangeParam value = dateCaptor.getValue();
    assertThat(value, notNullValue());
    assertThat(value.getUpperBound(), nullValue());
    assertThat(value.getLowerBound(), equalTo(new DateParam("ge2020-12-01")));
  }

  @Test
  public void testTriggerBulkExportJob() throws BulkExportException {
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
        fhirSearchUtil.triggerBulkExportJob(resourceTypes, FhirVersionEnum.R4);

    assertThat(contentLocationUrl, equalTo(mockLocationUrl));
    Mockito.verify(fetchUtil, times(1)).performServerOperation(any(), any(), any());
  }

  @Test(expected = BulkExportException.class)
  public void testTriggerBulkExportJobError() throws BulkExportException {
    List<String> resourceTypes = Arrays.asList("Patient,Observation,Encounter");
    MethodOutcome methodOutcome = new MethodOutcome();
    methodOutcome.setResponseStatusCode(HttpStatus.SC_BAD_REQUEST);
    when(fetchUtil.performServerOperation(any(), any(), any())).thenReturn(methodOutcome);

    fhirSearchUtil.triggerBulkExportJob(resourceTypes, FhirVersionEnum.R4);
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
        fhirSearchUtil.fetchBulkExportHttpResponse(mockLocationUrl);

    assertThat(bulkExportHttpResponse.getHttpStatus(), equalTo(HttpStatus.SC_OK));
    assertThat(bulkExportHttpResponse.getRetryAfter(), equalTo(120));
    assertThat(
        bulkExportHttpResponse.getExpires(), equalTo(new Date("Mon, 22 Jul 2019 23:59:59 GMT")));
    ObjectMapper objectMapper = new ObjectMapper();
    assertThat(
        bulkExportHttpResponse.getBulkExportResponse(),
        equalTo(objectMapper.readValue(bulkResponseString, BulkExportResponse.class)));
  }
}
