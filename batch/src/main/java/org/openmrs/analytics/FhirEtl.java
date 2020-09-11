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
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.DomainResource;
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
		 * By default, this reads from the MySQL DB `openmrs` at the default port on localhost.
		 */
		@Description("OpenMRS server URL")
		@Required
		@Default.String("http://localhost:8080/openmrs")
		String getServerUrl();
		
		void setServerUrl(String value);
		
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
		
		// TODO(bashir2): Switch to BasicAuth instead of relying on cookies.
		@Description("JSESSIONID cookie value to circumvent OpenMRS server authentication.")
		@Required
		String getJsessionId();
		
		void setJsessionId(String value);
		
		@Description("The sink GCP FHIR store with the format: "
		        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
		        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
		@Required
		String getGcpFhirStore();
		
		void setGcpFhirStore(String value);
		
		// TODO(bashir2): Add options for writing to various data warehouses or file types.
	}
	
	static FhirSearchUtil creatFhirSearchUtil(FhirEtlOptions options) {
		FhirContext fhirContext = FhirContext.forR4();
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(options.getGcpFhirStore());
		return new FhirSearchUtil(fhirStoreUtil, fhirContext);
	}
	
	static List<SearchSegmentDescriptor> createSegments(FhirEtlOptions options) throws CannotProvideCoderException {
		FhirSearchUtil fhirSearchUtil = creatFhirSearchUtil(options);
		List<SearchSegmentDescriptor> segments = new ArrayList<>();
		FhirContext fhirContext = FhirContext.forR4();
		for (String resourceType : options.getSearchList().split(",")) {
			String searchUrl = options.getServerUrl() + "/ws/fhir2/R4/" + resourceType;
			Bundle searchBundle = fhirSearchUtil.searchForResource(searchUrl, options.getBatchSize(), 0,
			    options.getJsessionId(), true, fhirContext);
			if (searchBundle == null) {
				log.error("Cannot fetch resources for " + searchUrl);
				throw new IllegalStateException("Cannot fetch resources for " + searchUrl);
			}
			int total = searchBundle.getTotal();
			log.info(String.format("Number of resources for %s search is %d", resourceType, total));
			if (searchBundle.getEntry().size() >= total) {
				// No parallelism is needed in this case; we have all the resources already.
				fhirSearchUtil.uploadBundleToCloud(searchBundle);
			} else {
				String baseNextUrl = fhirSearchUtil.findBaseSearchUrl(searchBundle);
				for (int offset = 0; offset < total; offset += options.getBatchSize()) {
					segments.add(SearchSegmentDescriptor.create(baseNextUrl, offset, options.getBatchSize(),
					    options.getJsessionId()));
				}
			}
		}
		log.info("Total number of segments is " + segments.size());
		return segments;
	}
	
	static class FetchSearchPageFn extends DoFn<SearchSegmentDescriptor, DomainResource> {
		
		private String gcpFhirStore;
		
		private FhirContext fhirContext;
		
		private FhirSearchUtil fhirSearchUtil;
		
		private FhirStoreUtil fhirStoreUtil;
		
		FetchSearchPageFn(String gcpFhirStore) {
			this.gcpFhirStore = gcpFhirStore;
		}
		
		@Setup
		public void Setup() {
			this.fhirContext = FhirContext.forR4();
			this.fhirStoreUtil = new FhirStoreUtil(gcpFhirStore);
			this.fhirSearchUtil = new FhirSearchUtil(fhirStoreUtil, fhirContext);
		}
		
		@ProcessElement
		public void ProcessElement(@Element SearchSegmentDescriptor segment, OutputReceiver<DomainResource> out) {
			Bundle pageBundle = fhirSearchUtil.searchForResource(segment.searchUrl(), segment.count(), segment.pageOffset(),
			    segment.jsessionId(), true, fhirContext);
			fhirSearchUtil.uploadBundleToCloud(pageBundle);
		}
	}
	
	static void runFhirFetch(FhirEtlOptions options) throws CannotProvideCoderException {
		List<SearchSegmentDescriptor> segments = createSegments(options);
		if (segments.isEmpty()) {
			return;
		}
		
		Pipeline p = Pipeline.create(options);
		p.apply(Create.of(segments)).apply(ParDo.of(new FetchSearchPageFn(options.getGcpFhirStore())));
		p.run().waitUntilFinish();
	}
	
	public static void main(String[] args) throws CannotProvideCoderException {
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		
		runFhirFetch(options);
	}
}
