// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.openmrs.analytics;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.DateClientParam;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirSearchUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FhirSearchUtil.class);
	
	private final OpenmrsUtil openmrsUtil;
	
	FhirSearchUtil(OpenmrsUtil openmrsUtil) {
		this.openmrsUtil = openmrsUtil;
	}
	
	public Bundle searchByUrl(String searchUrl, int count, SummaryEnum summaryMode) {
		try {
			IGenericClient client = openmrsUtil.getSourceClient();
			Bundle result = client.search().byUrl(searchUrl).count(count).summaryMode(summaryMode).returnBundle(Bundle.class)
			        .execute();
			return result;
		}
		catch (Exception e) {
			log.error("Failed to search for url: " + searchUrl + " ;  " + "Exception: " + e);
		}
		return null;
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
			throw new IllegalArgumentException(String.format("No proper link information in bundle %s", searchBundle));
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
			return openmrsUtil.getSourceFhirUrl() + "?" + pagesParam.toString();
		}
		catch (URISyntaxException e) {
			throw new IllegalArgumentException(
			        String.format("Malformed link information with error %s in bundle %s", e.getMessage(), searchBundle));
		}
	}
	
	public String findSelfUrl(Bundle searchBundle) {
		String selfLink = null;
		
		if (searchBundle.getLink(Bundle.LINK_SELF) != null) {
			selfLink = searchBundle.getLink(Bundle.LINK_SELF).getUrl();
		}
		
		if (selfLink == null) {
			throw new IllegalArgumentException(String.format("No proper self link information in bundle %s", searchBundle));
		}
		return selfLink;
	}
	
	private IQuery<Bundle> makeQueryForResource(String resourceType, int count) {
		IGenericClient client = openmrsUtil.getSourceClient(true);
		return client.search().forResource(resourceType).count(count).summaryMode(SummaryEnum.DATA)
		        .returnBundle(Bundle.class);
	}
	
	static List<String> getDateRange(String activePeriod) {
		if (activePeriod.isEmpty()) {
			return Lists.newArrayList();
		}
		String[] dateRange = activePeriod.split("_");
		if (dateRange.length != 1 && dateRange.length != 2) {
			throw new IllegalArgumentException("Invalid activePeriod '" + activePeriod
			        + "'; the format should be: DATE1_DATE2 or DATE1; see the flag description.");
		}
		return Lists.newArrayList(dateRange);
	}
	
	private IQuery<Bundle> makeQueryWithDate(String resourceType, FhirEtlOptions options) {
		IQuery<Bundle> searchQuery = makeQueryForResource(resourceType, options.getBatchSize());
		if (!options.getActivePeriod().isEmpty()) {
			List<String> dateRange = getDateRange(options.getActivePeriod());
			searchQuery = searchQuery.where(new DateClientParam("date").after().second(dateRange.get(0)));
			if (dateRange.size() > 1) {
				searchQuery = searchQuery.where(new DateClientParam("date").beforeOrEquals().second(dateRange.get(1)));
				log.info(String.format("Fetching all %s resources after %s and beforeOrEqual %s", resourceType,
				    dateRange.get(0), dateRange.get(1)));
			} else {
				log.info(String.format("Fetching all %s resources after %s", resourceType, dateRange.get(0)));
			}
		}
		return searchQuery;
	}
	
	Map<String, List<SearchSegmentDescriptor>> createSegments(FhirEtlOptions options) {
		if (options.getBatchSize() > 100) {
			// TODO: Probe this value from the server and set the maximum automatically.
			log.warn("NOTE batchSize flag is higher than 100; make sure that `fhir2.paging.maximum` "
			        + "is set accordingly on the OpenMRS server.");
		}
		Map<String, List<SearchSegmentDescriptor>> segmentMap = new HashMap<>();
		
		boolean anyResourceWithDate = false;
		for (String resourceType : options.getResourceList().split(",")) {
			List<SearchSegmentDescriptor> segments = new ArrayList<>();
			segmentMap.put(resourceType, segments);
			IQuery<Bundle> searchQuery = makeQueryWithDate(resourceType, options);
			log.info(String.format("Fetching first batch of %s", resourceType));
			Bundle searchBundle = null;
			try {
				searchBundle = searchQuery.execute();
				anyResourceWithDate = true;
			}
			catch (InvalidRequestException e) {
				log.warn(String.format("While searching for resource %s with date, caught exception %s", resourceType,
				    e.toString()));
				log.info("Trying without date for resource " + resourceType);
				searchBundle = makeQueryForResource(resourceType, options.getBatchSize()).execute();
			}
			if (searchBundle == null) {
				log.error("Failed searching for resource " + resourceType);
				throw new IllegalStateException("Failed searching for resource " + resourceType);
			}
			int total = searchBundle.getTotal();
			log.info(String.format("Number of resources for %s search is %d", resourceType, total));
			if (searchBundle.getEntry().size() >= total) {
				// No parallelism is needed in this case; we get all resources in one bundle.
				segments.add(SearchSegmentDescriptor.create(findSelfUrl(searchBundle), options.getBatchSize()));
			} else {
				String baseUrl = findBaseSearchUrl(searchBundle) + "&_getpagesoffset=";
				for (int offset = 0; offset < total; offset += options.getBatchSize()) {
					String pageSearchUrl = baseUrl + offset;
					segments.add(SearchSegmentDescriptor.create(pageSearchUrl, options.getBatchSize()));
				}
				log.info(String.format("Total number of segments for resource %s is %d", resourceType, segments.size()));
			}
		}
		if (!options.getActivePeriod().isEmpty() && !anyResourceWithDate) {
			throw new IllegalArgumentException(
			        String.format("None of the provided %s resources supported the 'date' options in the active period %s",
			            options.getResourceList(), options.getActivePeriod()));
		}
		return segmentMap;
	}
	
	public Set<String> findPatientAssociatedResources(Set<String> resourceTypes) {
		// Note this can also be done by fetching server's `metadata` and parsing the CapabilityStatement.
		Set<String> patientAssociatedResources = Sets.newHashSet();
		IGenericClient client = openmrsUtil.getSourceClient(true);
		for (String resourceType : resourceTypes) {
			IQuery<Bundle> query = client.search().forResource(resourceType).count(0).summaryMode(SummaryEnum.DATA)
			        .returnBundle(Bundle.class);
			// Try with a test ID and a random date to see whether the server can handle the search query.
			query = query.where(new ReferenceClientParam("patient").hasId("THIS_IS_FOR_TEST"))
			        .where(new DateClientParam("date").after().second("2021-01-01T00:00:00"));
			try {
				query.execute();
				patientAssociatedResources.add(resourceType);
			}
			catch (InvalidRequestException e) {
				log.warn(String.format(
				    "Server does not have 'patient' search param for resource %s; will not fetch historical %s resources.",
				    resourceType, resourceType));
			}
		}
		return patientAssociatedResources;
	}
	
	Bundle searchByPatientAndLastDate(String resourceType, String patientId, String lastDate, int count) {
		IGenericClient client = openmrsUtil.getSourceClient(true);
		IQuery<Bundle> query = client.search().forResource(resourceType).count(count).summaryMode(SummaryEnum.DATA)
		        .returnBundle(Bundle.class);
		query = query.where(new ReferenceClientParam("patient").hasId(patientId))
		        .where(new DateClientParam("date").beforeOrEquals().second(lastDate));
		return query.execute();
	}
}
