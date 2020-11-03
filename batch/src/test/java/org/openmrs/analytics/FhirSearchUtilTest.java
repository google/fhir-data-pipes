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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IResource;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import com.google.common.io.Resources;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirSearchUtilTest {
	
	private static final String PAGE_URL_PARAM = "_getpages=861af9b4-d847-4831-b945-ab1f6f08f03e";
	
	private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R3";
	
	private static final String SEARCH_URL = "Patient?given=TEST";
	
	private String bundleStr;
	
	@Mock
	private OpenmrsUtil openmrsUtil;
	
	@Mock
	private FhirStoreUtil fhirStoreUtil;
	
	@Mock
	private IGenericClient genericClient;
	
	@Mock
	private IUntypedQuery untypedQuery;
	
	@Mock
	private IQuery query;
	
	private Bundle bundle;
	
	private FhirContext fhirContext;
	
	private FhirSearchUtil fhirSearchUtil;

	private MethodOutcome outcome;
	
	@Before
	public void setup() throws IOException {
		URL url = Resources.getResource("bundle.json");
		bundleStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forDstu3();
		IParser parser = fhirContext.newJsonParser();
		bundle = parser.parseResource(Bundle.class, bundleStr);
		fhirSearchUtil = new FhirSearchUtil(fhirStoreUtil, openmrsUtil);
		outcome = new MethodOutcome();
		outcome.setCreated(true);
		when(openmrsUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
		when(openmrsUtil.getSourceClient()).thenReturn(genericClient);
		when(genericClient.search()).thenReturn(untypedQuery);
		when(untypedQuery.byUrl(SEARCH_URL)).thenReturn(query);
		when(query.count(anyInt())).thenReturn(query);
		when(query.summaryMode(any(SummaryEnum.class))).thenReturn(query);
		when(query.returnBundle(any())).thenReturn(query);
		when(query.execute()).thenReturn(bundle);
		when(fhirStoreUtil.uploadResourceToCloud(any())).thenReturn(outcome);
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
	public void testUploadBundleToCloud() {
		MethodOutcome result = fhirSearchUtil.uploadBundleToCloud(bundle);

		assertThat(result, not(nullValue()));
		assertThat(result.getCreated(), equalTo(true));
	}
}
