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

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ca.uhn.fhir.rest.api.SummaryEnum;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Property;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function object fetches all input segments from the FHIR source and writes each resource to
 * a Parquet file. If a FHIR sink address is provided, those resources are passed to that FHIR sink
 * too. The output PCollection contains, as keys, patient-ids extracted from the fetched resources,
 * with 1s as the value to make it easy to count in downstream functors.
 */
public class FetchResources extends PTransform<PCollection<SearchSegmentDescriptor>, PCollection<KV<String, Integer>>> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchResources.class);
	
	private static final Pattern PATIENT_REFERENCE = Pattern.compile(".*Patient/([^/]+)");
	
	@VisibleForTesting
	protected FetchSearchPageFn<SearchSegmentDescriptor> fetchSearchPageFn;
	
	FetchResources(FhirEtlOptions options, String stageIdentifier) {
		fetchSearchPageFn = new SearchFn(options, stageIdentifier);
	}
	
	@VisibleForTesting
	static String getSubjectPatientIdOrNull(Resource resource) {
		String patientId = null;
		Property subject = resource.getNamedProperty("subject");
		if (subject != null) {
			List<Base> values = subject.getValues();
			if (values.size() == 1) {
				Reference reference = (Reference) values.get(0);
				// TODO: Find a more generic way to check if this is a reference to a Patient. With the
				// current OpenMRS setup, reference.getType() is null so we cannot rely on that.
				String refStr = reference.getReference();
				Matcher matcher = PATIENT_REFERENCE.matcher(refStr);
				if (matcher.matches()) {
					patientId = matcher.group(1);
				}
				if (patientId == null) {
					log.warn(String.format("Ignoring subject of %s with id %s because it is not a Patient reference: %s",
					    resource.getResourceType(), resource.getId(), refStr));
				}
			}
			if (values.size() > 1) {
				log.warn(String.format("Unexpected multiple values for subject of %s with id %s", resource.getResourceType(),
				    resource.getId()));
			}
		}
		return patientId;
	}
	
	@Override
	public PCollection<KV<String, Integer>> expand(PCollection<SearchSegmentDescriptor> segments) {
		return segments.apply(ParDo.of(fetchSearchPageFn));
	}
	
	static class SearchFn extends FetchSearchPageFn<SearchSegmentDescriptor> {
		
		SearchFn(FhirEtlOptions options, String stageIdentifier) {
			super(options, stageIdentifier);
		}
		
		@ProcessElement
		public void processElement(@Element SearchSegmentDescriptor segment, OutputReceiver<KV<String, Integer>> out)
		        throws IOException {
			String searchUrl = segment.searchUrl();
			log.info(String.format("Fetching %d resources for statge %s; URL= %s", segment.count(), this.stageIdentifier,
			    searchUrl.substring(0, Math.min(200, searchUrl.length()))));
			long fetchStartTime = System.currentTimeMillis();
			Bundle pageBundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), SummaryEnum.DATA);
			addFetchTime(System.currentTimeMillis() - fetchStartTime);
			processBundle(pageBundle);
			if (pageBundle != null && pageBundle.getEntry() != null) {
				for (BundleEntryComponent entry : pageBundle.getEntry()) {
					String patientId = getSubjectPatientIdOrNull(entry.getResource());
					if (patientId != null) {
						out.output(KV.of(patientId, 1));
					}
				}
			}
		}
	}
}
