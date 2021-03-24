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
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.hl7.fhir.r4.model.Bundle;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam pipeline for reading FHIR resources from OpenMRS and pushing them into a data warehouse.
 */
public class FhirEtl {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEtl.class);
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createOpenmrsUtil(options.getOpenmrsServerUrl() + options.getServerFhirEndpoint(),
		    options.getOpenmrsUserName(), options.getOpenmrsPassword(), fhirContext));
	}
	
	static OpenmrsUtil createOpenmrsUtil(String sourceUrl, String sourceUser, String sourcePw, FhirContext fhirContext) {
		return new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
	}
	
	static Map<String, List<SearchSegmentDescriptor>> createSegments(FhirEtlOptions options, FhirContext fhirContext) {
		if (options.getBatchSize() > 100) {
			// TODO: Probe this value from the server and set the maximum automatically.
			log.warn("NOTE batchSize flag is higher than 100; make sure that `fhir2.paging.maximum` "
			        + "is set accordingly on the OpenMRS server.");
		}
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
	
	static String findSearchedResource(String search) {
		int argsStart = search.indexOf('?');
		if (argsStart >= 0) {
			return search.substring(0, argsStart);
		}
		return search;
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
	
	private static void fetchSegments(PCollection<SearchSegmentDescriptor> inputSegments, String search,
	        FhirEtlOptions options) {
		String resourceType = findSearchedResource(search);
		ParquetUtil parquetUtil = new ParquetUtil(options.getOutputParquetPath());
		Schema schema = parquetUtil.getResourceSchema(resourceType);
		FetchSearchPageFn fetchSearchPageFn = new FetchSearchPageFn(options, resourceType);
		PCollectionTuple records = inputSegments.apply(ParDo.of(fetchSearchPageFn).withOutputTags(fetchSearchPageFn.avroTag,
		    TupleTagList.of(fetchSearchPageFn.jsonTag)));
		records.get(fetchSearchPageFn.avroTag).setCoder(AvroCoder.of(GenericRecord.class, schema));
		if (!options.getOutputParquetPath().isEmpty()) {
			PCollection<GenericRecord> windowedRecords = addWindow(records.get(fetchSearchPageFn.avroTag),
			    options.getSecondsToFlushFiles());
			// TODO: Make sure getOutputParquetPath() is a directory.
			String outputFile = options.getOutputParquetPath() + resourceType;
			ParquetIO.Sink sink = ParquetIO.sink(schema); // TODO add an option for .withCompressionCodec();
			windowedRecords.apply(FileIO.<GenericRecord> write().via(sink).to(outputFile).withSuffix(".parquet")
			        .withNumShards(options.getNumFileShards()));
			// TODO add Avro output option
			// apply("WriteToAvro", AvroIO.writeGenericRecords(schema).to(outputFile).withSuffix(".avro")
			//        .withNumShards(options.getNumParquetShards()));
		}
		if (!options.getOutputJsonPath().isEmpty()) {
			PCollection<String> windowedRecords = addWindow(records.get(fetchSearchPageFn.jsonTag),
			    options.getSecondsToFlushFiles());
			windowedRecords.apply("WriteToText",
			    TextIO.write().to(options.getOutputJsonPath() + resourceType).withSuffix(".txt"));
		}
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) throws CannotProvideCoderException {
		Map<String, List<SearchSegmentDescriptor>> segmentMap = createSegments(options, fhirContext);
		if (segmentMap.isEmpty()) {
			return;
		}
		
		Pipeline pipeline = Pipeline.create(options);
		for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
			PCollection<SearchSegmentDescriptor> inputSegments = pipeline.apply(Create.of(entry.getValue()));
			fetchSegments(inputSegments, entry.getKey(), options);
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
		Map<String, String> reverseMap = jdbcUtil.createFhirReverseMap(options.getSearchList(),
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
			fetchSegments(inputSegments, resourceType, options);
		}
		PipelineResult result = pipeline.run();
		result.waitUntilFinish();
		EtlUtils.logMetrics(result.metrics());
	}
	
	public static void main(String[] args)
	        throws CannotProvideCoderException, PropertyVetoException, IOException, SQLException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		if (options.getNumFileShards() == 0) {
			if (!options.getOutputParquetPath().isEmpty() || !options.getOutputJsonPath().isEmpty())
				log.warn("Setting --numFileShards=0 can hinder output file generation performance significantly!");
		}
		
		ParquetUtil.initializeAvroConverters();
		
		if (options.isJdbcModeEnabled()) {
			runFhirJdbcFetch(options, fhirContext);
		} else {
			runFhirFetch(options, fhirContext);
		}
		
	}
}
