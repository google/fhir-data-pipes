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

import javax.sql.DataSource;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import com.google.common.io.Resources;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.analytics.model.EventConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFhirModeTest extends TestCase {
	
	private static final String BASE_URL = "http://localhost:9020/openmrs/ws/fhir2/R3";
	
	private static final String SEARCH_URL = "Encounter";
	// "../utils/dbz_event_to_fhir_config.json"
	
	private String resourceStr;
	
	@Rule
	public transient TestPipeline testPipeline = TestPipeline.create();
	
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
	
	private JdbcFhirMode jdbcFhirMode;
	
	private ParquetUtil parquetUtil;
	
	private String basePath = "/tmp/JUNIT/Parquet/TEST/";
	
	@Mock
	private DataSource ds;
	
	@Before
	public void setup() throws IOException {
		URL url = Resources.getResource("encounter.json");
		resourceStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forDstu3();
		IParser parser = fhirContext.newJsonParser();
		resource = parser.parseResource(Encounter.class, resourceStr);
		fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
		jdbcFhirMode = new JdbcFhirMode();
		parquetUtil = new ParquetUtil(fhirContext);
		//		when(openmrsUtil.getSourceFhirUrl()).thenReturn(BASE_URL);
		//		when(openmrsUtil.getSourceClient()).thenReturn(genericClient);
		//				when(genericClient.search()).thenReturn(untypedQuery);
		//				when(untypedQuery.byUrl(SEARCH_URL)).thenReturn(query);
		//				when(query.count(anyInt())).thenReturn(query);
		//				when(query.summaryMode(any(SummaryEnum.class))).thenReturn(query);
		//				when(query.returnBundle(any())).thenReturn(query);
		//		//		//when(query.execute()).thenReturn(resource);
		
		FileUtils.cleanDirectory(new File(basePath));
		
	}
	
	@Test
	public void testGetJdbcConfig() throws PropertyVetoException {
		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs("").withValidation()
		        .as(FhirEtl.FhirEtlOptions.class);
		JdbcIO.DataSourceConfiguration config = jdbcFhirMode.getJdbcConfig(options);
		assertTrue(JdbcIO.PoolableDataSourceProvider.of(config).apply(null) instanceof PoolingDataSource);
	}
	
	@Test
	// TODO
	public void testCreateChunkRanges() throws PropertyVetoException {
		//		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs("").withValidation().as(FhirEtl.FhirEtlOptions.class);
		//		JdbcIO.DataSourceConfiguration config = jdbcFhirMode.getJdbcConfig(options);
		//		PCollection<String> rows =
		//		testPipeline.apply(
		//				JdbcIO.<String>read()
		//						.withDataSourceConfiguration(config)
		//						.withQuery(String.format("select name,id from %s where name = ?", "ss"))
		//						);
		//
		//			PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1L);
		//
		//
		//			testPipeline.run();
	}
	
	@Test
	// TODO
	public void testGenerateFhirUrl() {
		
	}
	
	@Test
	public void testSinkToParquet() {
		
		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs("--outputParquetBase=" + basePath).withValidation()
		        .as(FhirEtl.FhirEtlOptions.class);
		
		Resource resource = this.resource;
		String resourceType = resource.getResourceType().name();
		Schema schema = parquetUtil.getResourceSchema(resourceType);
		List<GenericRecord> genericRecordsList = Arrays.asList(parquetUtil.convertToAvro(resource));
		PCollection<GenericRecord> genericRecords = testPipeline
		        .apply(Create.of(genericRecordsList).withCoder(AvroCoder.of(GenericRecord.class, schema)));
		
		JdbcFhirMode.sinkToParquet(options, schema, resourceType, genericRecords);
		
		testPipeline.run();
		File file = new File(basePath + resourceType);
		assertTrue(file.exists());
		
	}
	
	@Test
	public void testCreateFhirReverseMap() throws IOException {
		// here we pass Encounters as such we expect visits to be included in the reverseMap as well
		String[] args = { "--tableFhirMapPath=../utils/dbz_event_to_fhir_config.json",
		        "--searchList=Patient,Encounter,Observation" };
		FhirEtl.FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
		        .as(FhirEtl.FhirEtlOptions.class);
		
		LinkedHashMap<String, String> reverseMap = jdbcFhirMode.createFhirReverseMap(options);
		// we expect 4 objects, and visit should be included
		assertTrue(reverseMap.size() == 4);// not 3
		assertFalse(reverseMap.get("visit").isEmpty());
		assertFalse(reverseMap.get("encounter").isEmpty());
		assertFalse(reverseMap.get("obs").isEmpty());
		assertFalse(reverseMap.get("person").isEmpty());
		
	}
	
	@Test
	public void testGetTableToFhirConfig() throws IOException {
		String path = "../utils/dbz_event_to_fhir_config.json";
		LinkedHashMap<String, EventConfiguration> tableToFhirConfig = jdbcFhirMode.getTableToFhirConfig(path);
		// test key mappings
		assertEquals("/Patient/{uuid}", tableToFhirConfig.get("patient").getLinkTemplates().get("fhir"));
		assertEquals("/Encounter/{uuid}", tableToFhirConfig.get("encounter").getLinkTemplates().get("fhir"));
		assertEquals("/Patient/{uuid}", tableToFhirConfig.get("patient").getLinkTemplates().get("fhir"));
		assertEquals("/Observation/{uuid}", tableToFhirConfig.get("obs").getLinkTemplates().get("fhir"));
	}
}
