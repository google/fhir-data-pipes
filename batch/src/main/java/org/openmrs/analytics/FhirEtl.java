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
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.SummaryEnum;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);
	
	/**
	 * Options supported by {@link FhirEtl}.
	 */
	public interface FhirEtlOptions extends PipelineOptions {
		
		/**
		 * By default, this reads from the OpenMRS instance `openmrs` at the default port on localhost.
		 */
		@Description("OpenMRS server URL")
		@Required
		@Default.String("http://localhost:8099/openmrs")
		String getOpenmrsServerUrl();
		
		void setOpenmrsServerUrl(String value);
		
		@Description("OpenMRS server fhir endpoint")
		@Default.String("/ws/fhir2/R4")
		String getServerFhirEndpoint();
		
		void setServerFhirEndpoint(String value);
		
		@Description("Comma separated list of resource and search parameters to fetch; in its simplest "
		        + "form this is a list of resources, e.g., `Patient,Encounter,Observation` but more "
		        + "complex search paths are possible too, e.g., `Patient?name=Susan.`"
		        + "Please note that complex search params doesn't work when JDBC mode is enabled.")
		@Default.String("Patient,Encounter,Observation")
		String getSearchList();
		
		void setSearchList(String value);
		
		@Description("The number of resources to be fetched in one API call. "
		        + "For the JDBC mode passing > 170 could result in HTTP 400 Bad Request")
		@Default.Integer(170)
		int getBatchSize();
		
		void setBatchSize(int value);
		
		@Description("For the JDBC mode, this is the size of each ID chunk. Setting high values will yield faster query "
		        + "execution.")
		@Default.Integer(10000)
		int getJdbcFetchSize();
		
		void setJdbcFetchSize(int value);
		
		@Description("Openmrs BasicAuth Username")
		@Default.String("admin")
		String getOpenmrsUserName();
		
		void setOpenmrsUserName(String value);
		
		@Description("Openmrs BasicAuth Password")
		@Default.String("Admin123")
		String getOpenmrsPassword();
		
		void setOpenmrsPassword(String value);
		
		@Description("The path to the target generic fhir store, or a GCP fhir store with the format: "
		        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
		        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
		@Required
		@Default.String("")
		String getFhirSinkPath();
		
		void setFhirSinkPath(String value);
		
		@Description("Sink BasicAuth Username")
		@Default.String("")
		String getSinkUserName();
		
		void setSinkUserName(String value);
		
		@Description("Sink BasicAuth Password")
		@Default.String("")
		String getSinkPassword();
		
		void setSinkPassword(String value);
		
		@Description("The base name for output Parquet file; for each resource, one fileset will be created.")
		@Default.String("")
		String getFileParquetPath();
		
		void setFileParquetPath(String value);
		
		/**
		 * JDBC DB settings: defaults values have been pointed to ./openmrs-compose.yaml
		 */
		
		@Description("JDBC URL input")
		@Default.String("jdbc:mysql://localhost:3306/openmrs")
		String getJdbcUrl();
		
		void setJdbcUrl(String value);
		
		@Description("JDBC MySQL driver class")
		@Default.String("com.mysql.cj.jdbc.Driver")
		String getJdbcDriverClass();
		
		void setJdbcDriverClass(String value);
		
		@Description("JDBC maximum pool size")
		@Default.Integer(50)
		int getJdbcMaxPoolSize();
		
		void setJdbcMaxPoolSize(int value);
		
		@Description("MySQL DB user")
		@Default.String("root")
		String getDbUser();
		
		void setDbUser(String value);
		
		@Description("MySQL DB user password")
		@Default.String("debezium")
		String getDbPassword();
		
		void setDbPassword(String value);
		
		@Description("Path to Table-FHIR map config")
		@Default.String("utils/dbz_event_to_fhir_config.json")
		String getTableFhirMapPath();
		
		void setTableFhirMapPath(String value);
		
		@Description("Flag to switch between the 2 modes of batch extract")
		@Default.Boolean(false)
		Boolean isJdbcModeEnabled();
		
		void setJdbcModeEnabled(Boolean value);
	}
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createOpenmrsUtil(options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(),
		    options.getOpenmrsUserName(), options.getOpenmrsPassword(), fhirContext));
	}
	
	static OpenmrsUtil createOpenmrsUtil(String sourceUrl, String sourceUser, String sourcePw, FhirContext fhirContext) {
		return new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
	}
	
	static Map<String, List<SearchSegmentDescriptor>> createSegments(FhirEtlOptions options, FhirContext fhirContext)
	        throws CannotProvideCoderException {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Map<String, List<SearchSegmentDescriptor>> segmentMap = new HashMap<>();
		for (String search : options.getSearchList().split(",")) {
			List<SearchSegmentDescriptor> segments = new ArrayList<>();
			segmentMap.put(search, segments);
			String searchUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + search;
			log.info("searchUrl is " + searchUrl);
			Bundle searchBundle = fhirSearchUtil.searchByUrl(searchUrl, options.getBatchSize(), SummaryEnum.DATA);
			if (searchBundle == null) {
				log.error("Cannot fetch resources for " + searchUrl);
				throw new IllegalStateException("Cannot fetch resources for " + searchUrl);
			}
			int total = searchBundle.getTotal();
			log.info(String.format("Number of resources for %s search is %d", search, total));
			if (searchBundle.getEntry().size() >= total) {
				// No parallelism is needed in this case; we get all resources in one bundle.
				segments.add(SearchSegmentDescriptor.create(searchUrl, options.getBatchSize()));
			} else {
				for (int offset = 0; offset < total; offset += options.getBatchSize()) {
					String pageSearchUrl = fhirSearchUtil.findBaseSearchUrl(searchBundle) + "&_getpagesoffset=" + offset;
					segments.add(SearchSegmentDescriptor.create(pageSearchUrl, options.getBatchSize()));
				}
				log.info(String.format("Total number of segments for search %s is %d", search, segments.size()));
			}
		}
		return segmentMap;
	}
	
	static class FetchSearchPageFn extends DoFn<SearchSegmentDescriptor, GenericRecord> {
		
		private String sourceUrl;
		
		private String sourceUser;
		
		private String sourcePw;
		
		private String sinkPath;
		
		private String sinkUsername;
		
		private String sinkPassword;
		
		private String parquetFile;
		
		private ParquetUtil parquetUtil;
		
		private FhirContext fhirContext;
		
		private FhirSearchUtil fhirSearchUtil;
		
		private FhirStoreUtil fhirStoreUtil;
		
		private OpenmrsUtil openmrsUtil;
		
		FetchSearchPageFn(String fhirSinkPath, String sinkUsername, String sinkPassword, String sourceUrl,
		    String parquetFile, String sourceUser, String sourcePw) {
			this.sinkPath = fhirSinkPath;
			this.sinkUsername = sinkUsername;
			this.sinkPassword = sinkPassword;
			this.sourceUrl = sourceUrl;
			this.sourceUser = sourceUser;
			this.sourcePw = sourcePw;
			this.parquetFile = parquetFile;
		}
		
		@Setup
		public void Setup() {
			fhirContext = FhirContext.forR4();
			fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
			    fhirContext.getRestfulClientFactory());
			openmrsUtil = createOpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
			fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
			parquetUtil = new ParquetUtil(parquetFile);
		}
		
		@ProcessElement
		public void ProcessElement(@Element SearchSegmentDescriptor segment, OutputReceiver<GenericRecord> out) {
			log.debug("Fetching bundle: " + segment.searchUrl());
			Bundle pageBundle = fhirSearchUtil.searchByUrl(segment.searchUrl(), segment.count(), SummaryEnum.DATA);
			if (!parquetFile.isEmpty()) {
				for (GenericRecord record : parquetUtil.generateRecords(pageBundle)) {
					out.output(record);
				}
			}
			if (!sinkPath.isEmpty()) {
				fhirStoreUtil.uploadBundle(pageBundle);
			}
		}
	}
	
	static String findSearchedResource(String search) {
		int argsStart = search.indexOf('?');
		if (argsStart >= 0) {
			return search.substring(0, argsStart);
		}
		return search;
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) throws CannotProvideCoderException {
		ParquetUtil parquetUtil = new ParquetUtil(options.getFileParquetPath());
		Map<String, List<SearchSegmentDescriptor>> segmentMap = createSegments(options, fhirContext);
		if (segmentMap.isEmpty()) {
			return;
		}
		
		Pipeline p = Pipeline.create(options);
		for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
			String outputFile = "";
			if (!options.getFileParquetPath().isEmpty()) {
				outputFile = options.getFileParquetPath() + entry.getKey();
			}
			String resourceType = findSearchedResource(entry.getKey());
			Schema schema = parquetUtil.getResourceSchema(resourceType);
			PCollection<SearchSegmentDescriptor> inputSegments = p.apply(Create.of(entry.getValue()));
			
			PCollection<GenericRecord> records = inputSegments
			        .apply(ParDo.of(new FetchSearchPageFn(options.getFhirSinkPath(), options.getSinkUserName(),
			                options.getSinkPassword(), options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(),
			                options.getFileParquetPath(), options.getOpenmrsUserName(), options.getOpenmrsPassword())))
			        .setCoder(AvroCoder.of(GenericRecord.class, schema));
			ParquetIO.Sink sink = ParquetIO.sink(schema);
			records.apply(FileIO.<GenericRecord> write().via(sink).to(outputFile));
		}
		p.run().waitUntilFinish();
	}
	
	static void runFhirJdbcFetch(FhirEtlOptions options, FhirContext fhirContext)
	        throws PropertyVetoException, IOException, SQLException {
		Pipeline pipeline = Pipeline.create(options);
		JdbcFetchUtil jdbcUtil = new JdbcFetchUtil(options.getJdbcDriverClass(), options.getJdbcUrl(), options.getDbUser(),
		        options.getDbPassword(), options.getJdbcMaxPoolSize());
		JdbcIO.DataSourceConfiguration jdbcConfig = jdbcUtil.getJdbcConfig();
		int batchSize = Math.min(options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
		int jdbcFetchSize = options.getJdbcFetchSize();
		ParquetUtil parquetUtil = new ParquetUtil(options.getFileParquetPath());
		Map<String, String> reverseMap = jdbcUtil.createFhirReverseMap(options.getSearchList(),
		    options.getTableFhirMapPath());
		// process each table-resource mappings
		for (Map.Entry<String, String> entry : reverseMap.entrySet()) {
			String tableName = entry.getKey();
			String resourceType = entry.getValue();
			String baseBundleUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + resourceType;
			Schema schema = parquetUtil.getResourceSchema(resourceType);
			int maxId = jdbcUtil.fetchMaxId(tableName);
			Map<Integer, Integer> IdRanges = jdbcUtil.createIdRanges(maxId, jdbcFetchSize);
			PCollection<GenericRecord> genericRecords = pipeline.apply(Create.of(IdRanges))
			        .apply(new JdbcFetchUtil.FetchUuids(tableName, jdbcConfig))
			        .apply(new JdbcFetchUtil.CreateSearchSegments(resourceType, baseBundleUrl, batchSize))
			        .apply(ParDo.of(new FetchSearchPageFn(options.getFhirSinkPath(), options.getSinkUserName(),
			                options.getSinkPassword(), options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(),
			                options.getFileParquetPath(), options.getOpenmrsUserName(), options.getOpenmrsPassword())))
			        .setCoder(AvroCoder.of(GenericRecord.class, schema));
			
			// TODO: create a unified method without passing
			if (!options.getFileParquetPath().isEmpty()) {
				String outputFile = options.getFileParquetPath() + resourceType;
				ParquetIO.Sink sink = ParquetIO.sink(schema);
				genericRecords.apply(String.format("Saving parquet files: %s", outputFile),
				    FileIO.<GenericRecord> write().via(sink).to(outputFile));
			}
		}
		
		PipelineResult.State result = pipeline.run().waitUntilFinish();
		System.out.println(result);
	}
	
	public static void main(String[] args)
	        throws CannotProvideCoderException, PropertyVetoException, IOException, SQLException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		
		ParquetUtil.initializeAvroConverters();
		
		if (options.isJdbcModeEnabled()) {
			runFhirJdbcFetch(options, fhirContext);
		} else {
			runFhirFetch(options, fhirContext);
		}
		
	}
}
