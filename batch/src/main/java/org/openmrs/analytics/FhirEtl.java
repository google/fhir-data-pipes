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
import java.util.Collections;
import java.util.HashSet;
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
	
	/**
	 * This map is used when the activePeriod feature is enabled in the JDBC mode. For each table it
	 * indicates the column on which the date filter is applied. It is best if these columns are not
	 * nullable and there is an index on them.
	 */
	private static final Map<String, String> tableDateColumn;
	static {
		Map<String, String> tempMap = Maps.newHashMap();
		tempMap.put("encounter", "encounter_datetime");
		tempMap.put("obs", "obs_datetime");
		tempMap.put("visit", "date_started");
		tableDateColumn = Collections.unmodifiableMap(tempMap);
	}
	
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
			//        .withNumShards(options.getnumFileShards()));
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
	
	static void fetchPatientHistory(Pipeline pipeline, List<PCollection<KV<String, Integer>>> allPatientIds,
	        Set<String> patientAssociatedResources, FhirEtlOptions options) {
		PCollectionList<KV<String, Integer>> patientIdList = PCollectionList.<KV<String, Integer>> empty(pipeline)
		        .and(allPatientIds);
		PCollection<KV<String, Integer>> flattenedPatients = patientIdList.apply(Flatten.pCollections());
		PCollection<KV<String, Integer>> mergedPatients = flattenedPatients.apply(Sum.integersPerKey());
		final String patientType = "Patient";
		FetchPatients fetchPatients = new FetchPatients(options, getSchema(patientType));
		PCollectionTuple patientRecords = mergedPatients.apply(fetchPatients);
		writeToParquet(fetchPatients.getAvroRecords(patientRecords), options, patientType, "active");
		writeToJson(fetchPatients.getJsonRecords(patientRecords), options, patientType, "active");
		for (String resourceType : patientAssociatedResources) {
			FetchPatientHistory fetchPatientHistory = new FetchPatientHistory(options, resourceType,
			        getSchema(resourceType));
			PCollectionTuple records = mergedPatients.apply(fetchPatientHistory);
			writeToParquet(fetchPatientHistory.getAvroRecords(records), options, resourceType, "history");
			writeToJson(fetchPatientHistory.getJsonRecords(records), options, resourceType, "history");
		}
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Map<String, List<SearchSegmentDescriptor>> segmentMap = Maps.newHashMap();
		try {
			// TODO in the activePeriod case, among patientAssociatedResources, only fetch Encounter here.
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
			allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
		}
		if (!options.getActivePeriod().isEmpty()) {
			Set<String> patientAssociatedResources = fhirSearchUtil.findPatientAssociatedResources(segmentMap.keySet());
			fetchPatientHistory(pipeline, allPatientIds, patientAssociatedResources, options);
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		EtlUtils.logMetrics(result.metrics());
	}
	
	static void runFhirJdbcFetch(FhirEtlOptions options, FhirContext fhirContext)
	        throws PropertyVetoException, IOException, SQLException, CannotProvideCoderException {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Pipeline pipeline = Pipeline.create(options);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(), options.getJdbcUrl(),
		        options.getDbUser(), options.getDbPassword(), options.getJdbcMaxPoolSize(),
		        options.getJdbcInitialPoolSize());
		JdbcFetchUtil jdbcUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		int batchSize = Math.min(options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
		Map<String, List<String>> reverseMap = jdbcUtil.createFhirReverseMap(options.getResourceList(),
		    options.getTableFhirMapPath());
		// process each table-resource mappings
		Set<String> resourceTypes = new HashSet<>();
		List<PCollection<KV<String, Integer>>> allPatientIds = Lists.newArrayList();
		for (Map.Entry<String, List<String>> entry : reverseMap.entrySet()) {
			String tableName = entry.getKey();
			log.info(String.format("List of resources for table %s is %s", tableName, entry.getValue()));
			PCollection<String> uuids;
			if (options.getActivePeriod().isEmpty() || !tableDateColumn.containsKey(tableName)) {
				if (!options.getActivePeriod().isEmpty()) {
					log.warn(String.format("There is no date mapping for table %s; fetching all rows.", tableName));
				}
				uuids = jdbcUtil.fetchAllUuids(pipeline, tableName, options.getJdbcFetchSize());
			} else {
				uuids = jdbcUtil.fetchUuidsByDate(pipeline, tableName, tableDateColumn.get(tableName),
				    options.getActivePeriod());
			}
			for (String resourceType : entry.getValue()) {
				resourceTypes.add(resourceType);
				String baseBundleUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint() + "/" + resourceType;
				PCollection<SearchSegmentDescriptor> inputSegments = uuids.apply(
				    String.format("CreateSearchSegments_%s_table_%s", resourceType, tableName),
				    new JdbcFetchUtil.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
				allPatientIds.add(fetchSegmentsAndReturnPatientIds(inputSegments, resourceType, options));
			}
		}
		if (!options.getActivePeriod().isEmpty()) {
			Set<String> patientAssociatedResources = fhirSearchUtil.findPatientAssociatedResources(resourceTypes);
			fetchPatientHistory(pipeline, allPatientIds, patientAssociatedResources, options);
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
			Set<String> resourceSet = Sets.newHashSet(options.getResourceList().split(","));
			if (resourceSet.contains("Patient")) {
				throw new IllegalArgumentException(
				        "When using --activePeriod feature, 'Patient' should not be in --resourceList got: "
				                + options.getResourceList());
			}
			if (!resourceSet.contains("Encounter")) {
				throw new IllegalArgumentException(
				        "When using --activePeriod feature, 'Encounter' should be in --resourceList got: "
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
			runFhirJdbcFetch(options, fhirContext);
		} else {
			runFhirFetch(options, fhirContext);
		}
		
	}
}
