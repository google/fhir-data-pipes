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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.SummaryEnum;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.FileIO;
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
import org.hl7.fhir.dstu3.model.Bundle;
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
		String getServerUrl();
		
		void setServerUrl(String value);
		
		@Description("OpenMRS server fhir endpoint")
		@Required
		@Default.String("/ws/fhir2/R3")
		String getServerFhirEndpoint();
		
		void setServerFhirEndpoint(String value);
		
		@Description("Comma separated list of resource and search parameters to fetch; in its simplest "
		        + "form this is a list of resources, e.g., `Patient,Encounter,Observation` but more "
		        + "complex search paths are possible too, e.g., `Patient?name=Susan`.")
		@Default.String("Patient,Encounter,Observation")
		String getSearchList();
		
		void setSearchList(String value);
		
		@Description("The number of resources to be fetched in one API call.")
		@Default.Integer(10)
		int getBatchSize();
		
		void setBatchSize(int value);
		
		@Description("Openmrs BasicAuth Username")
		@Default.String("admin")
		String getUsername();
		
		void setUsername(String value);
		
		@Description("Openmrs BasicAuth Password")
		@Default.String("Admin123")
		@Required
		String getPassword();
		
		void setPassword(String value);
		
		@Description("The target fhir store OR GCP FHIR store with the format: "
		        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
		        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
		// TODO set the default value of this to empty string once FhirSearchUtil is refactored and GCP
		// specific parts are taken out. Then add the support for having both FHIR store and Parquet
		// output enabled at the same time.
		@Default.String("projects/P/locations/L/datasets/D/fhirStores/F")
		String getSinkPath();
		
		void setSinkPath(String value);
		
		@Description("Sink BasicAuth Username")
		@Default.String("hapi")
		String getSinkUsername();
		
		void setSinkUsername(String value);
		
		@Description("Sink BasicAuth Password")
		@Default.String("hapi")
		String getSinkPassword();
		
		void setSinkPassword(String value);
		
		@Description("The base name for output Parquet file; for each resource, one fileset will be created.")
		@Default.String("")
		String getOutputParquetBase();
		
		void setOutputParquetBase(String value);
	}
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createOpenmrsUtil(options.getServerUrl() + options.getServerFhirEndpoint(),
		    options.getUsername(), options.getPassword(), fhirContext));
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
			String searchUrl = options.getServerUrl() + options.getServerFhirEndpoint() + "/" + search;
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
		
		FetchSearchPageFn(String sinkPath, String sinkUsername, String sinkPassword, String sourceUrl, String parquetFile,
		    String sourceUser, String sourcePw) {
			this.sinkPath = sinkPath;
			this.sinkUsername = sinkUsername;
			this.sinkPassword = sinkPassword;
			this.sourceUrl = sourceUrl;
			this.sourceUser = sourceUser;
			this.sourcePw = sourcePw;
			this.parquetFile = parquetFile;
		}
		
		@Setup
		public void Setup() {
			fhirContext = FhirContext.forDstu3();
			fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
			    fhirContext.getRestfulClientFactory());
			openmrsUtil = createOpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
			fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
			parquetUtil = new ParquetUtil(fhirContext);
		}
		
		@ProcessElement
		public void ProcessElement(@Element SearchSegmentDescriptor segment, OutputReceiver<GenericRecord> out) {
			Bundle pageBundle = fhirSearchUtil.searchByUrl(segment.searchUrl(), segment.count(), SummaryEnum.DATA);
			if (parquetFile.isEmpty()) {
				fhirStoreUtil.uploadBundle(pageBundle);
			} else {
				for (GenericRecord record : parquetUtil.generateRecords(pageBundle)) {
					out.output(record);
				}
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
		ParquetUtil parquetUtil = new ParquetUtil(fhirContext);
		Map<String, List<SearchSegmentDescriptor>> segmentMap = createSegments(options, fhirContext);
		if (segmentMap.isEmpty()) {
			return;
		}
		
		Pipeline p = Pipeline.create(options);
		for (Map.Entry<String, List<SearchSegmentDescriptor>> entry : segmentMap.entrySet()) {
			String outputFile = "";
			if (!options.getOutputParquetBase().isEmpty()) {
				outputFile = options.getOutputParquetBase() + entry.getKey();
			}
			String resourceType = findSearchedResource(entry.getKey());
			Schema schema = parquetUtil.getResourceSchema(resourceType);
			PCollection<SearchSegmentDescriptor> inputSegments = p.apply(Create.of(entry.getValue()));
			PCollection<GenericRecord> records = inputSegments
			        .apply(ParDo.of(new FetchSearchPageFn(options.getSinkPath(), options.getSinkUsername(),
			                options.getSinkPassword(), options.getServerUrl() + options.getServerFhirEndpoint(),
			                options.getOutputParquetBase(), options.getUsername(), options.getPassword())))
			        .setCoder(AvroCoder.of(GenericRecord.class, schema));
			ParquetIO.Sink sink = ParquetIO.sink(schema);
			records.apply(FileIO.<GenericRecord> write().via(sink).to(outputFile));
		}
		p.run().waitUntilFinish();
	}
	
	public static void main(String[] args) throws CannotProvideCoderException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forDstu3();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		
		ParquetUtil.initializeAvroConverters();
		
		runFhirFetch(options, fhirContext);
	}
}
