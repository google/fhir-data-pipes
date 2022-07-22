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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.openmrs.analytics.model.DatabaseConfiguration;
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
		return new OpenmrsUtil(options.getFhirServerUrl(), 
				options.getFhirServerUserName(), options.getFhirServerPassword(), fhirContext);
	}
	
	private static Schema getSchema(String resourceType) {
		ParquetUtil parquetUtil = new ParquetUtil(null); // This is used only to get schema.
		return parquetUtil.getResourceSchema(resourceType);
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
		FetchResources fetchResources = new FetchResources(options, resourceType + "_main");
		return inputSegments.apply(fetchResources);
	}
	
	static void fetchPatientHistory(Pipeline pipeline, List<PCollection<KV<String, Integer>>> allPatientIds,
	        Set<String> patientAssociatedResources, FhirEtlOptions options) {
		PCollectionList<KV<String, Integer>> patientIdList = PCollectionList.<KV<String, Integer>> empty(pipeline)
		        .and(allPatientIds);
		PCollection<KV<String, Integer>> flattenedPatients = patientIdList.apply(Flatten.pCollections());
		PCollection<KV<String, Integer>> mergedPatients = flattenedPatients.apply(Sum.integersPerKey());
		final String patientType = "Patient";
		FetchPatients fetchPatients = new FetchPatients(options, getSchema(patientType));
		mergedPatients.apply(fetchPatients);
		for (String resourceType : patientAssociatedResources) {
			FetchPatientHistory fetchPatientHistory = new FetchPatientHistory(options, resourceType);
			mergedPatients.apply(fetchPatientHistory);
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
	
	static void runFhirJdbcFetch(FhirEtlOptions options, DatabaseConfiguration dbConfig, FhirContext fhirContext)
	        throws PropertyVetoException, IOException, SQLException, CannotProvideCoderException {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		Pipeline pipeline = Pipeline.create(options);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(),
		        dbConfig.makeJdbsUrlFromConfig(), dbConfig.getDbUser(), dbConfig.getDbPassword(),
		        options.getJdbcInitialPoolSize(), options.getJdbcMaxPoolSize());
		JdbcFetchUtil jdbcUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		int batchSize = Math.min(options.getBatchSize(), 170); // batch size > 200 will result in HTTP 400 Bad Request
		Map<String, List<String>> reverseMap = jdbcUtil.createFhirReverseMap(options.getResourceList(), dbConfig);
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
				String baseBundleUrl = options.getFhirServerUrl() + "/" 
						+ resourceType;
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
	
	/**
	 * Driver function for running JDBC direct fetch mode with a HAPI source server. The JDBC fetch mode
	 * uses Beam JdbcIO to fetch FHIR resources directly from the HAPI server's database and uses the
	 * ParquetUtil to write resources.
	 */
	static void runHapiJdbcFetch(FhirEtlOptions options, DatabaseConfiguration dbConfig, FhirContext fhirContext)
	        throws PropertyVetoException {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(options.getJdbcDriverClass(),
		        dbConfig.makeJdbsUrlFromConfig(), dbConfig.getDbUser(), dbConfig.getDbPassword(),
		        options.getJdbcInitialPoolSize(), options.getJdbcMaxPoolSize());
		JdbcFetchUtil jdbcFetchUtil = new JdbcFetchUtil(jdbcConnectionUtil);
		Pipeline pipeline = Pipeline.create(options);
		
		String searchUrl = options.getFhirServerUrl() + "/$get-resource-counts";
		HashSet<String> resourceTypes = new HashSet<String>(Arrays.asList(options.getResourceList().split(",")));
		//Get the resource count for each resource type and distribute the query workload evenly across allocated workers.
		HashMap<String, Integer> resourceCount = fhirSearchUtil.searchResourceCounts(searchUrl, resourceTypes);
		PCollection<List<String>> payload = jdbcFetchUtil.runJdbcIoFetch(pipeline, options.getResourceList(), resourceCount,
		    options.getJdbcMaxPoolSize());
		payload.apply("Convert to parquet", ParDo.of(new ConvertResourceFn(options)));
		
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		EtlUtils.logMetrics(result.metrics());
	}
	
	public static void main(String[] args)
	        throws CannotProvideCoderException, PropertyVetoException, IOException, SQLException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		ParquetUtil.initializeAvroConverters();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		log.info("Flags: " + options);
		validateOptions(options);
		
		if (options.isJdbcModeEnabled()) {
			DatabaseConfiguration dbConfig = DatabaseConfiguration.createConfigFromFile(options.getFhirDebeziumConfigPath());
			
			if (options.isJdbcModeHapi()) {
				runHapiJdbcFetch(options, dbConfig, fhirContext);
			} else {
				runFhirJdbcFetch(options, dbConfig, fhirContext);
			}
			
		} else {
			runFhirFetch(options, fhirContext);
		}
		log.info("DONE!");
	}
}
