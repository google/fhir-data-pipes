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

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.dstu3.model.Encounter;
import org.hl7.fhir.dstu3.model.Resource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFetchUtilTest extends TestCase {
	
	private String resourceStr;
	
	@Rule
	public transient TestPipeline testPipeline = TestPipeline.create();
	
	private Resource resource;
	
	private FhirContext fhirContext;
	
	private JdbcFetchUtil jdbcFetchUtil;
	
	private ParquetUtil parquetUtil;
	
	private String basePath = "/tmp/JUNIT/Parquet/TEST/";
	
	@Before
	public void setup() throws IOException, PropertyVetoException {
		URL url = Resources.getResource("encounter.json");
		resourceStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forDstu3();
		IParser parser = fhirContext.newJsonParser();
		resource = parser.parseResource(Encounter.class, resourceStr);
		
		String[] args = { "--fhirSinkPath=", "--openmrsServerUrl=http://localhost:8099/openmrs" };
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(), options.getJdbcUrl(),
		        options.getDbUser(), options.getDbPassword(), options.getJdbcMaxPoolSize(),
		        options.getJdbcInitialPoolSize());
		// TODO jdbcConnectionUtil should be replaced by a mocked JdbcConnectionUtil which does not
		// depend on options either, since we don't need real DB connections for unit-testing.
		jdbcFetchUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		parquetUtil = new ParquetUtil(basePath);
		// clean up if folder exists
		File file = new File(basePath);
		if (file.exists())
			FileUtils.cleanDirectory(file);
	}
	
	@Test
	public void testGetJdbcConfig() throws PropertyVetoException {
		JdbcIO.DataSourceConfiguration config = jdbcFetchUtil.getJdbcConfig();
		assertTrue(JdbcIO.PoolableDataSourceProvider.of(config).apply(null) instanceof PoolingDataSource);
	}
	
	@Test
	public void testCreateIdRanges() {
		int batchSize = 100;
		int maxId = 200;
		Map<Integer, Integer> idRanges = jdbcFetchUtil.createIdRanges(maxId, batchSize);
		Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
		expectedMap.put(101, 200);
		expectedMap.put(1, 100);
		assertEquals(idRanges, expectedMap);
	}
	
	@Test
	public void testCreateSearchSegmentDescriptor() {
		
		String resourceType = "Encounter";
		String baseBundleUrl = "https://test.com/" + resourceType;
		int batchSize = 2;
		String[] uuIds = { "<uuid>", "<uuid>", "<uuid>", "<uuid>", "<uuid>", "<uuid>" };
		PCollection<SearchSegmentDescriptor> createdSegments = testPipeline
		        .apply("Create input", Create.of(Arrays.asList(uuIds)))
		        // Inject
		        .apply(new JdbcFetchUtil.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
		// create expected output
		List<SearchSegmentDescriptor> segments = new ArrayList<>();
		// first batch
		segments.add(SearchSegmentDescriptor
		        .create(String.format("%s?_id=%s", baseBundleUrl, String.join(",", new String[] { "<uuid>,<uuid>" })), 2));
		// second batch
		segments.add(SearchSegmentDescriptor
		        .create(String.format("%s?_id=%s", baseBundleUrl, String.join(",", new String[] { "<uuid>,<uuid>" })), 2));
		// third batch
		segments.add(SearchSegmentDescriptor
		        .create(String.format("%s?_id=%s", baseBundleUrl, String.join(",", new String[] { "<uuid>,<uuid>" })), 2));
		// assert
		PAssert.that(createdSegments).containsInAnyOrder(segments);
		testPipeline.run();
	}
	
	@Test
	public void testCreateFhirReverseMap() throws Exception {
		Map<String, List<String>> reverseMap = jdbcFetchUtil.createFhirReverseMap("Patient,Person,Encounter,Observation",
		    "../utils/dbz_event_to_fhir_config.json");
		
		assertEquals(reverseMap.size(), 4);
		assertEquals(reverseMap.get("person").size(), 2);
		assertTrue(reverseMap.get("person").contains("Patient"));
		assertTrue(reverseMap.get("person").contains("Person"));
		assertTrue(reverseMap.get("encounter").contains("Encounter"));
		assertTrue(reverseMap.get("visit").contains("Encounter"));
		assertTrue(reverseMap.get("obs").contains("Observation"));
	}
}
