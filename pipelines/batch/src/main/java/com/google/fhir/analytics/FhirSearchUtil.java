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
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SearchTotalModeEnum;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IHttpResponse;
import ca.uhn.fhir.rest.gclient.DateClientParam;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import ca.uhn.fhir.rest.param.DateRangeParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.exception.BulkExportException;
import com.google.fhir.analytics.model.BulkExportHttpResponse;
import com.google.fhir.analytics.model.BulkExportResponse;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hl7.fhir.instance.model.api.IBaseParameters;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirSearchUtil {

  private static final Logger log = LoggerFactory.getLogger(FhirSearchUtil.class);

  private final FetchUtil fetchUtil;

  FhirSearchUtil(FetchUtil fetchUtil) {
    this.fetchUtil = fetchUtil;
  }

  public Bundle searchByUrl(String searchUrl, int count, SummaryEnum summaryMode) {
    try {
      IGenericClient client = fetchUtil.getSourceClient();
      Bundle result =
          client
              .search()
              .byUrl(searchUrl)
              .count(count)
              .summaryMode(summaryMode)
              .returnBundle(Bundle.class)
              .execute();
      return result;
    } catch (Exception e) {
      log.error("Failed to search for url: " + searchUrl + " ;  " + "Exception: " + e);
    }
    return null;
  }

  /**
   * Searches for the total number of resources for each resource type
   *
   * @param resourceList the resource types to be processed
   * @return a Map storing the counts of each resource type
   */
  public Map<String, Integer> searchResourceCounts(String resourceList, String since) {
    HashSet<String> resourceTypes = new HashSet<String>(Arrays.asList(resourceList.split(",")));
    HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
    for (String resourceType : resourceTypes) {
      try {
        String searchUrl = resourceType + "?";
        IGenericClient client = fetchUtil.getSourceClient();
        IQuery<Bundle> query =
            client
                .search()
                .byUrl(searchUrl)
                .summaryMode(SummaryEnum.COUNT)
                .returnBundle(Bundle.class);
        if (since != null && !since.isEmpty()) {
          query.lastUpdated(new DateRangeParam(since, null));
        }
        Bundle result = query.execute();
        hashMap.put(resourceType, result.getTotal());
        log.info("Number of {} resources = {}", resourceType, result.getTotal());
      } catch (Exception e) {
        log.error("Failed to search for resource: " + resourceType + " ;  " + "Exception: " + e);
        throw e;
      }
    }

    return hashMap;
  }

  public String getNextUrl(Bundle bundle) {
    if (bundle != null && bundle.getLink(Bundle.LINK_NEXT) != null) {
      return bundle.getLink(Bundle.LINK_NEXT).getUrl();
    }
    return null;
  }

  @VisibleForTesting
  String findBaseSearchUrl(Bundle searchBundle) {
    String searchLink = getNextUrl(searchBundle);
    if (searchLink == null) {
      throw new IllegalArgumentException(
          String.format("No proper link information in bundle %s", searchBundle));
    }

    try {
      URI searchUri = new URI(searchLink);
      NameValuePair pagesParam = null;
      for (NameValuePair pair : URLEncodedUtils.parse(searchUri, StandardCharsets.UTF_8)) {
        if (pair.getName().equals("_getpages")) {
          pagesParam = pair;
        }
      }
      if (pagesParam == null) {
        throw new IllegalArgumentException(
            String.format("No _getpages parameter found in search link %s", searchLink));
      }
      return fetchUtil.getSourceFhirUrl() + "?" + pagesParam.toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Malformed link information with error %s in bundle %s",
              e.getMessage(), searchBundle));
    }
  }

  public String findSelfUrl(Bundle searchBundle) {
    String selfLink = null;

    if (searchBundle.getLink(Bundle.LINK_SELF) != null) {
      selfLink = searchBundle.getLink(Bundle.LINK_SELF).getUrl();
    }

    if (selfLink == null) {
      throw new IllegalArgumentException(
          String.format("No proper self link information in bundle %s", searchBundle));
    }
    return selfLink;
  }

  private IQuery<Bundle> makeQueryForResource(String resourceType, int count) {
    IGenericClient client = fetchUtil.getSourceClient(true);
    return client
        .search()
        .forResource(resourceType)
        .totalMode(SearchTotalModeEnum.ACCURATE)
        .count(count)
        .summaryMode(SummaryEnum.DATA)
        .returnBundle(Bundle.class);
  }

  static List<String> getDateRange(String activePeriod) {
    if (activePeriod.isEmpty()) {
      return Lists.newArrayList();
    }
    String[] dateRange = activePeriod.split("_");
    if (dateRange.length != 1 && dateRange.length != 2) {
      throw new IllegalArgumentException(
          "Invalid activePeriod '"
              + activePeriod
              + "'; the format should be: DATE1_DATE2 or DATE1; see the flag description.");
    }
    return Lists.newArrayList(dateRange);
  }

  private IQuery<Bundle> makeQueryWithDate(String resourceType, FhirEtlOptions options) {
    IQuery<Bundle> searchQuery = makeQueryForResource(resourceType, options.getBatchSize());
    if (!Strings.isNullOrEmpty(options.getActivePeriod())) {
      List<String> dateRange = getDateRange(options.getActivePeriod());
      searchQuery = searchQuery.where(new DateClientParam("date").after().second(dateRange.get(0)));
      if (dateRange.size() > 1) {
        searchQuery =
            searchQuery.where(
                new DateClientParam("date").beforeOrEquals().second(dateRange.get(1)));
        log.info(
            String.format(
                "Fetching all %s resources after %s and beforeOrEqual %s",
                resourceType, dateRange.get(0), dateRange.get(1)));
      } else {
        log.info(
            String.format("Fetching all %s resources after %s", resourceType, dateRange.get(0)));
      }
    }
    if (!Strings.isNullOrEmpty(options.getSince())) {
      searchQuery.lastUpdated(new DateRangeParam(options.getSince(), null));
    }
    return searchQuery;
  }

  Map<String, List<SearchSegmentDescriptor>> createSegments(FhirEtlOptions options) {
    if (options.getBatchSize() > 100) {
      // TODO: Probe this value from the server and set the maximum automatically.
      log.warn(
          "NOTE batchSize flag is higher than 100; make sure that the FHIR server supports "
              + "search page sizes that large (the default maximum is sometime 100).");
    }
    Map<String, List<SearchSegmentDescriptor>> segmentMap = new HashMap<>();

    boolean anyResourceWithDate = false;
    for (String resourceType : options.getResourceList().split(",")) {
      List<SearchSegmentDescriptor> segments = new ArrayList<>();
      IQuery<Bundle> searchQuery = makeQueryWithDate(resourceType, options);
      log.info(String.format("Fetching first batch of %s", resourceType));
      Bundle searchBundle = null;
      try {
        searchBundle = searchQuery.execute();
        anyResourceWithDate = true;
      } catch (InvalidRequestException e) {
        log.warn(
            String.format(
                "While searching for resource %s with date, caught exception %s",
                resourceType, e.toString()));
        log.info("Trying without date for resource " + resourceType);
        searchBundle = makeQueryForResource(resourceType, options.getBatchSize()).execute();
      }
      if (searchBundle == null) {
        log.error("Failed searching for resource " + resourceType);
        throw new IllegalStateException("Failed searching for resource " + resourceType);
      }
      int total = searchBundle.getTotal();
      log.info(String.format("Number of resources for %s search is %d", resourceType, total));
      if (total == 0) {
        continue; // Nothing to be done for this resource type.
      }
      if (searchBundle.getEntry().size() >= total) {
        // No parallelism is needed in this case; we get all resources in one bundle.
        segments.add(
            SearchSegmentDescriptor.create(findSelfUrl(searchBundle), options.getBatchSize()));
      } else {
        // TODO: This is HAPI specific and should be generalized:
        //  https://github.com/google/fhir-data-pipes/issues/533
        String baseUrl = findBaseSearchUrl(searchBundle) + "&_getpagesoffset=";
        for (int offset = 0; offset < total; offset += options.getBatchSize()) {
          String pageSearchUrl = baseUrl + offset;
          segments.add(SearchSegmentDescriptor.create(pageSearchUrl, options.getBatchSize()));
        }
        log.info(
            String.format(
                "Total number of segments for resource %s is %d", resourceType, segments.size()));
      }
      segmentMap.put(resourceType, segments);
    }
    if (!options.getActivePeriod().isEmpty() && !anyResourceWithDate) {
      throw new IllegalArgumentException(
          String.format(
              "None of the provided %s resources supported the 'date' options in the active period"
                  + " %s",
              options.getResourceList(), options.getActivePeriod()));
    }
    return segmentMap;
  }

  public Set<String> findPatientAssociatedResources(Set<String> resourceTypes) {
    // Note this can also be done by fetching server's `metadata` and parsing the
    // CapabilityStatement.
    Set<String> patientAssociatedResources = Sets.newHashSet();
    IGenericClient client = fetchUtil.getSourceClient(true);
    for (String resourceType : resourceTypes) {
      IQuery<Bundle> query =
          client
              .search()
              .forResource(resourceType)
              .count(0)
              .summaryMode(SummaryEnum.DATA)
              .returnBundle(Bundle.class);
      // Try with a test ID and a random date to see whether the server can handle the search query.
      query =
          query
              .where(new ReferenceClientParam("patient").hasId("THIS_IS_FOR_TEST"))
              .where(new DateClientParam("date").after().second("2021-01-01T00:00:00"));
      try {
        query.execute();
        patientAssociatedResources.add(resourceType);
      } catch (InvalidRequestException e) {
        log.warn(
            String.format(
                "Server does not have 'patient' search param for resource %s; will not fetch"
                    + " historical %s resources.",
                resourceType, resourceType));
      }
    }
    return patientAssociatedResources;
  }

  Bundle searchByPatientAndLastDate(
      String resourceType, String patientId, String lastDate, int count) {
    IGenericClient client = fetchUtil.getSourceClient(true);
    IQuery<Bundle> query =
        client
            .search()
            .forResource(resourceType)
            .count(count)
            .summaryMode(SummaryEnum.DATA)
            .returnBundle(Bundle.class);
    query =
        query
            .where(new ReferenceClientParam("patient").hasId(patientId))
            .where(new DateClientParam("date").beforeOrEquals().second(lastDate));
    return query.execute();
  }

  /**
   * Fetch the details of bulk export job for the given bulkExportStatusUrl
   *
   * @param bulkExportStatusUrl the url of the bulk export job
   * @return BulkExportHttpResponse - the status and details of the bulk export job
   * @throws IOException
   */
  public BulkExportHttpResponse fetchBulkExportHttpResponse(String bulkExportStatusUrl)
      throws IOException {
    IHttpResponse httpResponse = fetchUtil.fetchResponseForUrl(bulkExportStatusUrl);
    BulkExportHttpResponse bulkExportHttpResponse = new BulkExportHttpResponse();
    bulkExportHttpResponse.setHttpStatus(httpResponse.getStatus());
    if (httpResponse.getHeaders("expires") != null
        && !httpResponse.getHeaders("expires").isEmpty()) {
      String expiresString = httpResponse.getHeaders("expires").get(0);
      Date expires = new Date(expiresString);
      bulkExportHttpResponse.setExpires(expires);
    }
    if (!CollectionUtils.isEmpty(httpResponse.getHeaders("retry-after"))) {
      String retryHeaderString = httpResponse.getHeaders("retry-after").get(0);
      bulkExportHttpResponse.setRetryAfter(Integer.valueOf(retryHeaderString));
    }
    if (!CollectionUtils.isEmpty(httpResponse.getHeaders("x-progress"))) {
      String xProgress = httpResponse.getHeaders("x-progress").get(0);
      bulkExportHttpResponse.setXProgress(xProgress);
    }

    String body = null;
    try (Reader reader = httpResponse.createReader()) {
      body = IOUtils.toString(reader);
    }

    if (!Strings.isNullOrEmpty(body)) {
      ObjectMapper objectMapper = new ObjectMapper();
      BulkExportResponse bulkExportResponse =
          objectMapper.readValue(body, BulkExportResponse.class);
      bulkExportHttpResponse.setBulkExportResponse(bulkExportResponse);
    }
    return bulkExportHttpResponse;
  }

  /**
   * Start the bulk export for the given resourceTypes with the fhirVersionEnum
   *
   * @param resourceTypes the types which needs to be exported
   * @param fhirVersionEnum the fhir version of the resources to be exported
   * @return the absolute url via which the status and details of the job can be fetched
   */
  public String triggerBulkExportJob(List<String> resourceTypes, FhirVersionEnum fhirVersionEnum)
      throws BulkExportException {
    Map<String, List<String>> headers = new HashMap<>();
    headers.put("accept", Arrays.asList("application/fhir+ndjson"));
    headers.put("prefer", Arrays.asList("respond-async"));
    MethodOutcome methodOutcome =
        fetchUtil.performServerOperation(
            "export", fetchBulkExportParameters(fhirVersionEnum, resourceTypes), headers);
    if ((methodOutcome.getResponseStatusCode() / 100) != 2) {
      throw new BulkExportException(
          String.format(
              "An error occurred while calling the bulk export API, statusCode=%s",
              methodOutcome.getResponseStatusCode()));
    }
    Optional<String> responseLocation = methodOutcome.getFirstResponseHeader("content-location");
    if (responseLocation.isEmpty()) {
      throw new BulkExportException("The content location for bulk export api is empty");
    }
    return responseLocation.get();
  }

  private IBaseParameters fetchBulkExportParameters(
      FhirVersionEnum fhirVersionEnum, List<String> resourceTypes) throws BulkExportException {
    switch (fhirVersionEnum) {
      case R4:
        Parameters r4Parameters = new Parameters();
        r4Parameters.addParameter("_type", String.join(",", resourceTypes));
        return r4Parameters;
      default:
        throw new BulkExportException(
            String.format("Fhir Version not supported yet for bulk export : %s", fhirVersionEnum));
    }
  }

  /**
   * Validates if a connection can be established to the FHIR server by executing a search query.
   */
  void testFhirConnection() {
    log.info("Validating FHIR connection");
    IGenericClient client = fetchUtil.getSourceClient();
    IQuery<Bundle> query =
        client
            .search()
            .forResource(Patient.class)
            .summaryMode(SummaryEnum.COUNT)
            .totalMode(SearchTotalModeEnum.ACCURATE)
            .returnBundle(Bundle.class);
    // The query is executed and checked for any errors during the connection, the result is ignored
    query.execute();
    log.info("Validating FHIR connection successful");
  }
}
