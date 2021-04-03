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
import java.nio.file.FileSystems;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createOpenmrsUtil(options, fhirContext));
	}
	
	static OpenmrsUtil createOpenmrsUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new OpenmrsUtil(options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(), options.getOpenmrsUserName(),
		        options.getOpenmrsPassword(), fhirContext);
	}
	
	static <T> PCollection<T> addWindow(PCollection<T> records, int secondsToFlush) {
		if (secondsToFlush <= 0) {
			return records;
		}
		// We are dealing with a bounded source hence we can simply use the single Global window.
		// Also we don't need to deal with late data.
		// TODO: Implement an easy way to unit-test this functionality.
		return records.apply(Window.<T> into(new GlobalWindows())
		        .triggering(Repeatedly.forever(
		            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(secondsToFlush))))
		        .discardingFiredPanes());
	}
	
	private static Schema getSchema(String resourceType) {
		ParquetUtil parquetUtil = new ParquetUtil(null); // This is used only to get schema.
		return parquetUtil.getResourceSchema(resourceType);
	}
	
	static void writeToParquet(PCollection<GenericRecord> records, FhirEtlOptions options, String resourceType,
	        String subDir) {
		if (!options.getOutputParquetPath().isEmpty()) {
			PCollection<GenericRecord> windowedRecords = addWindow(records, options.getSecondsToFlushFiles());
			String outputDir = FileSystems.getDefault().getPath(options.getOutputParquetPath(), resourceType, subDir)
			        .toString();
			log.info("Will write Parquet files to " + outputDir);
			ParquetIO.Sink sink = ParquetIO.sink(getSchema(resourceType)); // TODO add an option for .withCompressionCodec();
			windowedRecords.apply(FileIO.<GenericRecord> write().via(sink).to(outputDir).withSuffix(".parquet")
			        .withNumShards(options.getNumFileShards()));
			// TODO add Avro output option
			// apply("WriteToAvro", AvroIO.writeGenericRecords(schema).to(outputFile).withSuffix(".avro")
			//        .withNumShards(options.getNumParquetShards()));
		}
	}
	
	static void writeToJson(PCollection<String> records, FhirEtlOptions options, String resourceType, String subDir) {
		if (!options.getOutputJsonPath().isEmpty()) {
			PCollection<String> windowedRecords = addWindow(records, options.getSecondsToFlushFiles());
			String outputDir = FileSystems.getDefault().getPath(options.getOutputParquetPath(), resourceType, subDir)
			        .toString();
			log.info("Will write JSON files to " + outputDir);
			windowedRecords.apply("WriteToText", TextIO.write().to(outputDir).withSuffix(".txt"));
		}
	}
	
	/**
	 * For each SearchSegmentDescriptor, it fetches the given resources, convert them to output
	 * Parquet/JSON files, and output the IDs of the fetched resources.
	 *
	 * @param inputSegments each element defines a set of resources to be fetched in one FHIR call.
	 * @param resourceType the type of the resources, e.g., Patient or Observation
	 * @param options the pipeline options
	 * @return a PCollection of all patient IDs of fetched resources or empty if `resourceType` has no
	 *         patient ID association.
	 */
	private static PCollection<KV<String, Integer>> fetchSegmentsAndReturnPatientIds(
	        PCollection<SearchSegmentDescriptor> inputSegments, String resourceType, FhirEtlOptions options) {
		Schema schema = getSchema(resourceType);
		FetchResources fetchResources = new FetchResources(options, resourceType, schema);
		PCollectionTuple records = inputSegments.apply(fetchResources);
		writeToParquet(fetchResources.getAvroRecords(records), options, resourceType, "active");
		writeToJson(fetchResources.getJsonRecords(records), options, resourceType, "active");
		return fetchResources.getPatientIds(records);
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Map<String, List<SearchSegmentDescriptor>> segmentMap = Maps.newHashMap();
		try {
			segmentMap = fhirSearchUtil.createSegments(options);
		}
		catch (IllegalArgumentException e) {
			log.error("Either the date format in the active period is wrong or none of the resources support 'date' feature"
			        + e.getMessage());
			throw e;
		}
		if (segmentMap.isEmpty()) {
			return;
		}
		
		Pipeline pipeline = Pipeline.create(options);
		List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
		for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
			String resourceType = entry.getKey();
			PCollection<SearchSegmentDescriptor> inputSegments = pipeline.apply(Create.of(entry.getValue()));
			PCollection<KV<String, Integer>> patientIds = fetchSegmentsAndReturnPatientIds(inputSegments, resourceType,
			    options);
			if (!options.getActivePeriod().isEmpty()) {
				allPatientIds.add(patientIds);
			}
		}
		if (!options.getActivePeriod().isEmpty()) {
			Set<String> patientAssociatedResources = fhirSearchUtil.findPatientAssociatedResources(segmentMap.keySet());
			PCollectionList<KV<String, Integer>> patientIdList = PCollectionList.<KV<String, Integer>> empty(pipeline)
			        .and(allPatientIds);
			PCollection<KV<String, Integer>> flattenedPatients = patientIdList.apply(Flatten.pCollections());
			PCollection<KV<String, Integer>> mergedPatients = flattenedPatients.apply(Sum.integersPerKey());
			for (String resourceType : patientAssociatedResources) {
				FetchPatientHistory fetchPatientHistory = new FetchPatientHistory(options, resourceType,
				        getSchema(resourceType));
				PCollectionTuple records = mergedPatients.apply(fetchPatientHistory);
				writeToParquet(fetchPatientHistory.getAvroRecords(records), options, resourceType, "history");
				writeToJson(fetchPatientHistory.getJsonRecords(records), options, resourceType, "history");
			}
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		EtlUtils.logMetrics(result.metrics());
	}
	
	static void runFhirJdbcFetch(FhirEtlOptions options, FhirContext fhirContext)
	        throws PropertyVetoException, IOException, SQLException {
		Pipeline pipeline = Pipeline.create(options);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(), options.getJdbcUrl(),
		        options.getDbUser(), options.getDbPassword(), options.getJdbcMaxPoolSize(),
		        options.getJdbcInitialPoolSize());
		JdbcFetchUtil jdbcUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		JdbcIO.DataSourceConfiguration jdbcConfig = jdbcUtil.getJdbcConfig();
		int batchSize = Math.min(options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
		int jdbcFetchSize = options.getJdbcFetchSize();
		Map<String, String> reverseMap = jdbcUtil.createFhirReverseMap(options.getResourceList(),
		    options.getTableFhirMapPath());
		// process each table-resource mappings
		for (Map.Entry<String, String> entry : reverseMap.entrySet()) {
			String tableName = entry.getKey();
			String resourceType = entry.getValue();
			String baseBundleUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + resourceType;
			int maxId = jdbcUtil.fetchMaxId(tableName);
			Map<Integer, Integer> IdRanges = jdbcUtil.createIdRanges(maxId, jdbcFetchSize);
			PCollection<SearchSegmentDescriptor> inputSegments = pipeline.apply(Create.of(IdRanges))
			        .apply(new JdbcFetchUtil.FetchUuids(tableName, jdbcConfig))
			        .apply(new JdbcFetchUtil.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
			fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options);
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		EtlUtils.logMetrics(result.metrics());
	}
	
	static void validateOptions(FhirEtlOptions options) {
		if (options.getNumFileShards() == 0) {
			if (!options.getOutputParquetPath().isEmpty() || !options.getOutputJsonPath().isEmpty()) {
				log.warn("Setting --numFileShards=0 can hinder output file generation performance significantly!");
			}
		}
		
		if (!options.getActivePeriod().isEmpty()) {
			if (options.isJdbcModeEnabled()) {
				throw new IllegalArgumentException("--activePeriod is not supported in JDBC mode.");
			}
			Set<String> resourceSet = Sets.newHashSet(options.getResourceList().split(","));
			if (resourceSet.contains("Patinet")) {
				throw new IllegalArgumentException(
				        "Using --activePeriod feature requires 'Patient' to be in --resourceList got: "
				                + options.getResourceList());
			}
		}
	}
	
	public static void main(String[] args)
	        throws CannotProvideCoderException, PropertyVetoException, IOException, SQLException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		ParquetUtil.initializeAvroConverters();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		validateOptions(options);
		
		if (options.isJdbcModeEnabled()) {
			if (!options.getActivePeriod().isEmpty()) {
				log.error("The 'active period' feature is not supported in JDBC mode.");
				return;
			}
			runFhirJdbcFetch(options, fhirContext);
		} else {
			runFhirFetch(options, fhirContext);
		}
		
	}
}
