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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import com.google.common.io.Resources;
import junit.framework.TestCase;
import org.apache.beam.sdk.testing.TestPipeline;
import org.hl7.fhir.dstu3.model.Resource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFhirModeTest extends TestCase {
	
	private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R3";
	
	private static final String SEARCH_URL = "Encounter";
	// "../utils/dbz_event_to_fhir_config.json"
	
	private String resourceStr;
	
	@Rule
	public transient TestPipeline mainPipeline = TestPipeline.create();
	
	@Mock
	protected Statement st;
	
	@Mock
	protected Connection cn;
	
	@Mock
	protected PreparedStatement ps;
	
	@Mock
	private OpenmrsUtil openmrsUtil;
	
	@Mock
	private IGenericClient genericClient;
	
	@Mock
	private IUntypedQuery untypedQuery;
	
	@Mock
	private IQuery query;
	
	private Resource resource;
	
	private FhirContext fhirContext;
	
	private FhirSearchUtil fhirSearchUtil;
	
	@Before
	public void setup() throws IOException {
		URL url = Resources.getResource("encounter.json");
		resourceStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forDstu3();
		IParser parser = fhirContext.newJsonParser();
		//resource = parser.parseResource(Resource.class, resourceStr);
		//		fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
		//		when(openmrsUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
		//		when(openmrsUtil.getSourceClient()).thenReturn(genericClient);
		//		when(genericClient.search()).thenReturn(untypedQuery);
		//		when(untypedQuery.byUrl(SEARCH_URL)).thenReturn(query);
		//		when(query.count(anyInt())).thenReturn(query);
		//		when(query.summaryMode(any(SummaryEnum.class))).thenReturn(query);
		//		when(query.returnBundle(any())).thenReturn(query);
		//		//when(query.execute()).thenReturn(resource);
		
	}
	
	@Test
	public void testGetJdbcConfig() {
	}
	
	@Test
	public void testCreateChunkRanges() {
	}
	
	@Test
	public void testGenerateFhirUrl() {
	}
	
	@Test
	public void testCreateFhirReverseMap() {
	}
	
	@Test
	public void testGetTableToFhirConfig() {
	}
}
