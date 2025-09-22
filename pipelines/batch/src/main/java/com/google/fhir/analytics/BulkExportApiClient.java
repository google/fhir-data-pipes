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

import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IHttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportHttpResponse.BulkExportHttpResponseBuilder;
import com.google.fhir.analytics.model.BulkExportResponse;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.Reader;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.hl7.fhir.instance.model.api.IBaseParameters;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.jspecify.annotations.Nullable;

/**
 * This class contains methods to trigger and fetch the details of bulk export api on the FHIR
 * server.
 */
public class BulkExportApiClient {

  private static final String EXPIRES = "expires";
  private static final String RETRY_AFTER = "retry-after";
  private static final String X_PROGRESS = "x-progress";
  private static final String PARAMETER_TYPE = "_type";
  private static final String PARAMETER_SINCE = "_since";

  private final FetchUtil fetchUtil;

  /**
   * Starts the bulk export for the given resourceTypes and fhirVersionEnum
   *
   * @param resourceTypes the types which needs to be exported
   * @param since the fhir resources fetched should have been updated after this timestamp
   * @param fhirVersionEnum the fhir version of the resources to be exported
   * @return the absolute url via which the status and details of the job can be fetched
   */
  public String triggerBulkExportJob(
      List<String> resourceTypes, @Nullable String since, FhirVersionEnum fhirVersionEnum) {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put(HttpHeaders.ACCEPT, Arrays.asList("application/fhir+ndjson"));
    headers.put("Prefer", Arrays.asList("respond-async"));
    MethodOutcome methodOutcome =
        fetchUtil.performServerOperation(
            "export", fetchBulkExportParameters(fhirVersionEnum, resourceTypes, since), headers);
    if (!isStatusSuccessful(methodOutcome.getResponseStatusCode())) {
      throw new IllegalStateException(
          String.format(
              "An error occurred while calling the bulk export API, statusCode=%s",
              methodOutcome.getResponseStatusCode()));
    }
    Optional<String> responseLocation = methodOutcome.getFirstResponseHeader("content-location");
    if (responseLocation.isEmpty()) {
      throw new IllegalStateException("The content location for bulk export api is empty");
    }
    return responseLocation.get();
  }

  BulkExportApiClient(FetchUtil fetchUtil) {
    Preconditions.checkNotNull(fetchUtil, "fetchUtil cannot be null");
    this.fetchUtil = fetchUtil;
  }

  /**
   * Fetches the details of the bulk export job for the given bulkExportStatusUrl
   *
   * @param bulkExportStatusUrl the url of the bulk export job
   * @return BulkExportHttpResponse - the status and details of the bulk export job
   * @throws IOException in case of any error while fetching the status of bulk export job
   */
  public BulkExportHttpResponse fetchBulkExportHttpResponse(String bulkExportStatusUrl)
      throws IOException {
    IHttpResponse httpResponse = fetchUtil.fetchResponseForUrl(bulkExportStatusUrl);
    BulkExportHttpResponseBuilder httpResponseBuilder = BulkExportHttpResponse.builder();
    httpResponseBuilder.httpStatus(httpResponse.getStatus());
    if (httpResponse.getHeaders(EXPIRES) != null && !httpResponse.getHeaders(EXPIRES).isEmpty()) {
      String expiresString = httpResponse.getHeaders(EXPIRES).get(0);
      DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(expiresString, formatter);
      Date expires = Date.from(zonedDateTime.toInstant());
      httpResponseBuilder.expires(expires);
    }
    if (!CollectionUtils.isEmpty(httpResponse.getHeaders(RETRY_AFTER))) {
      String retryHeaderString = httpResponse.getHeaders(RETRY_AFTER).get(0);
      httpResponseBuilder.retryAfter(Integer.parseInt(retryHeaderString));
    }
    if (!CollectionUtils.isEmpty(httpResponse.getHeaders(X_PROGRESS))) {
      String xProgress = httpResponse.getHeaders(X_PROGRESS).get(0);
      httpResponseBuilder.xProgress(xProgress);
    }

    String body;
    try (Reader reader = httpResponse.createReader()) {
      body = IOUtils.toString(reader);
    }

    if (!Strings.isNullOrEmpty(body)) {
      Gson gson = new Gson();
      BulkExportResponse bulkExportResponse = gson.fromJson(body, BulkExportResponse.class);
      httpResponseBuilder.bulkExportResponse(bulkExportResponse);
    }
    return httpResponseBuilder.build();
  }

  private IBaseParameters fetchBulkExportParameters(
      FhirVersionEnum fhirVersionEnum, List<String> resourceTypes, @Nullable String since) {
    since = Strings.nullToEmpty(since);
    return switch (fhirVersionEnum) {
      case R4 -> {
        Parameters r4Parameters = new Parameters();
        r4Parameters.addParameter(PARAMETER_TYPE, String.join(",", resourceTypes));
        if (!since.isEmpty()) {
          r4Parameters.addParameter(PARAMETER_SINCE, new InstantType(since));
        }
        yield r4Parameters;
      }
      case DSTU3 -> {
        // TODO: Create a common interface to handle parameters of different versions
        org.hl7.fhir.dstu3.model.Parameters dstu3Parameters =
            new org.hl7.fhir.dstu3.model.Parameters();
        org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent typeParameter =
            new org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent();
        typeParameter.setName(PARAMETER_TYPE);
        typeParameter.setValue(
            new org.hl7.fhir.dstu3.model.StringType(String.join(",", resourceTypes)));
        dstu3Parameters.addParameter(typeParameter);

        if (!since.isEmpty()) {
          org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent sinceParameter =
              new org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent();
          sinceParameter.setName(PARAMETER_SINCE);
          sinceParameter.setValue(new org.hl7.fhir.dstu3.model.InstantType(since));
          dstu3Parameters.addParameter(sinceParameter);
        }
        yield dstu3Parameters;
      }
      default -> throw new IllegalArgumentException(
          String.format("Fhir Version not supported yet for bulk export : %s", fhirVersionEnum));
    };
  }

  private boolean isStatusSuccessful(int httpStatusCode) {
    return httpStatusCode / 100 == 2;
  }
}
