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
import ca.uhn.fhir.rest.api.SummaryEnum;
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
		 * By default, this reads from the OpenMRS instance `openmrs` at the default port on localhost.
		 */
		@Description("OpenMRS server URL")
		@Required
		String getServerUrl();
		
		void setServerUrl(String value);
		
		@Description("OpenMRS server fhir endpoint")
		@Required
		@Default.String("/ws/fhir2/R4")
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
		
		@Description("BasicAuth Username")
		@Default.String("admin")
		String getUsername();
		
		void setUsername(String value);
		
		@Description("BasicAuth Password")
		@Default.String("Admin123")
		@Required
		String getPassword();
		
		void setPassword(String value);
		
		@Description("The target fhir store OR GCP FHIR store with the format: "
		        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
		        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
		@Required
		String getFhirStoreUrl();
		
		void setFhirStoreUrl(String value);
	}
	
	static FhirSearchUtil createFhirSearchUtil(FhirEtlOptions options, FhirContext fhirContext) {
		return new FhirSearchUtil(createFhirStoreUtil(options.getFhirStoreUrl(), fhirContext),
		        createOpenmrsUtil(options.getServerUrl() + options.getServerFhirEndpoint(), options.getUsername(),
		            options.getPassword(), fhirContext));
	}
	
	static FhirStoreUtil createFhirStoreUtil(String fhirStoreUrl, FhirContext fhirContext) {
		return new FhirStoreUtil(fhirStoreUrl, fhirContext);
	}
	
	static OpenmrsUtil createOpenmrsUtil(String sourceUrl, String sourceUser, String sourcePw, FhirContext fhirContext) {
		return new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
	}
	
	static List<SearchSegmentDescriptor> createSegments(FhirEtlOptions options, FhirContext fhirContext) {
		FhirSearchUtil fhirSearchUtil = createFhirSearchUtil(options, fhirContext);
		List<SearchSegmentDescriptor> segments = new ArrayList<>();
		for (String resourceType : options.getSearchList().split(",")) {
			String searchUrl = options.getServerUrl() + options.getServerFhirEndpoint() + "/" + resourceType;
			Bundle searchBundle = fhirSearchUtil.searchForResource(resourceType, options.getBatchSize(), SummaryEnum.DATA);
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
				String pageId = fhirSearchUtil.findBaseSearchUrl(searchBundle);
				for (int offset = 0; offset < total; offset += options.getBatchSize()) {
					segments.add(SearchSegmentDescriptor.create(pageId, offset, options.getBatchSize()));
				}
			}
		}
		log.info("Total number of segments is " + segments.size());
		return segments;
	}
	
	static class FetchSearchPageFn extends DoFn<SearchSegmentDescriptor, DomainResource> {
		
		private String sourceUrl;
		
		private String sourceUser;
		
		private String sourcePw;
		
		private String fhirStoreUrl;
		
		private FhirSearchUtil fhirSearchUtil;
		
		private FhirStoreUtil fhirStoreUtil;
		
		private OpenmrsUtil openmrsUtil;
		
		private FhirContext fhirContext;
		
		FetchSearchPageFn(String fhirStoreUrl, String sourceUrl, String sourceUser, String sourcePw) {
			this.fhirStoreUrl = fhirStoreUrl;
			this.sourceUrl = sourceUrl;
			this.sourceUser = sourceUser;
			this.sourcePw = sourcePw;
		}
		
		@Setup
		public void Setup() {
			this.fhirContext = FhirContext.forR4();
			this.fhirStoreUtil = createFhirStoreUtil(this.fhirStoreUrl, this.fhirContext);
			this.openmrsUtil = createOpenmrsUtil(this.sourceUrl, this.sourceUser, this.sourcePw, this.fhirContext);
			this.fhirSearchUtil = new FhirSearchUtil(fhirStoreUtil, openmrsUtil);
		}
		
		@ProcessElement
		public void ProcessElement(@Element SearchSegmentDescriptor segment, OutputReceiver<DomainResource> out) {
			Bundle pageBundle = fhirSearchUtil.searchByPage(segment.pageId(), segment.count(), segment.pageOffset(),
			    SummaryEnum.DATA);
			fhirSearchUtil.uploadBundleToCloud(pageBundle);
		}
	}
	
	static void runFhirFetch(FhirEtlOptions options, FhirContext fhirContext) throws CannotProvideCoderException {
		List<SearchSegmentDescriptor> segments = createSegments(options, fhirContext);
		if (segments.isEmpty()) {
			return;
		}
		
		Pipeline p = Pipeline.create(options);
		p.apply(Create.of(segments)).apply(ParDo.of(new FetchSearchPageFn(options.getFhirStoreUrl(),
		        options.getServerUrl() + options.getServerFhirEndpoint(), options.getUsername(), options.getPassword())));
		p.run().waitUntilFinish();
	}
	
	public static void main(String[] args) throws CannotProvideCoderException {
		// Todo: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		
		FhirEtlOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
		
		runFhirFetch(options, fhirContext);
	}
}
