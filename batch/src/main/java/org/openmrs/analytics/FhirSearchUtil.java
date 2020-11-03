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

import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hl7.fhir.dstu3.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirSearchUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FhirSearchUtil.class);
	
	FhirStoreUtil fhirStoreUtil;
	
	private OpenmrsUtil openmrsUtil;
	
	FhirSearchUtil(OpenmrsUtil openmrsUtil) {
		this.openmrsUtil = openmrsUtil;
	}
	
	Bundle searchByUrl(String searchUrl, int count, SummaryEnum summaryMode) {
		try {
			IGenericClient client = openmrsUtil.getSourceClient();
			Bundle result = client.search().byUrl(searchUrl).count(count).summaryMode(summaryMode).returnBundle(Bundle.class)
			        .execute();
			return result;
		}
		catch (Exception e) {
			log.error("Failed to search for url:" + searchUrl + ";  " + "Exception: " + e);
		}
		return null;
	}
	
	public String findBaseSearchUrl(Bundle searchBundle) {
		String searchLink = null;
		
		if (searchBundle.getLink(Bundle.LINK_NEXT) != null) {
			searchLink = searchBundle.getLink(Bundle.LINK_NEXT).getUrl();
		}
		
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
	
}
