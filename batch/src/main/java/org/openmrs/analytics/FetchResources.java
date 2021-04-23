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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ca.uhn.fhir.rest.api.SummaryEnum;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hl7.fhir.r4.model.Base;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Property;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function object fetches all input segments from the FHIR source and converts each resource
 * to JSON or Avro (or both). If a FHIR sink address is provided, those resources are passed to that
 * FHIR sink too.
 */
public class FetchResources extends PTransform<PCollection<SearchSegmentDescriptor>, PCollectionTuple> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchResources.class);
	
	private static final Pattern PATIENT_REFERENCE = Pattern.compile(".*Patient/([^/]+)");
	
	final private Schema schema;
	
	@VisibleForTesting
	protected FetchSearchPageFn<SearchSegmentDescriptor> fetchSearchPageFn;
	
	static final private TupleTag<KV<String, Integer>> patientIdTag = new TupleTag<KV<String, Integer>>() {};
	
	FetchResources(FhirEtlOptions options, String stageIdentifier, Schema schema) {
		this.schema = schema;
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
	public PCollectionTuple expand(PCollection<SearchSegmentDescriptor> segments) {
		PCollectionTuple records = segments.apply(ParDo.of(fetchSearchPageFn).withOutputTags(fetchSearchPageFn.avroTag,
		    TupleTagList.of(Lists.newArrayList(fetchSearchPageFn.jsonTag, patientIdTag))));
		records.get(fetchSearchPageFn.avroTag).setCoder(AvroCoder.of(GenericRecord.class, schema));
		return records;
	}
	
	public PCollection<GenericRecord> getAvroRecords(PCollectionTuple records) {
		return fetchSearchPageFn.getAvroRecords(records);
	}
	
	public PCollection<String> getJsonRecords(PCollectionTuple records) {
		return fetchSearchPageFn.getJsonRecords(records);
	}
	
	public PCollection<KV<String, Integer>> getPatientIds(PCollectionTuple records) {
		return records.get(patientIdTag);
	}
	
	static class SearchFn extends FetchSearchPageFn<SearchSegmentDescriptor> {
		
		SearchFn(FhirEtlOptions options, String stageIdentifier) {
			super(options, stageIdentifier);
		}
		
		@ProcessElement
		public void processElement(@Element SearchSegmentDescriptor segment, MultiOutputReceiver out) {
			String searchUrl = segment.searchUrl();
			log.info(String.format("Fetching %d resources for statge %s; URL= %s", segment.count(), this.stageIdentifier,
			    searchUrl.substring(0, Math.min(200, searchUrl.length()))));
			long fetchStartTime = System.currentTimeMillis();
			Bundle pageBundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), SummaryEnum.DATA);
			addFetchTime(System.currentTimeMillis() - fetchStartTime);
			processBundle(pageBundle, out);
			if (pageBundle != null && pageBundle.getEntry() != null) {
				for (BundleEntryComponent entry : pageBundle.getEntry()) {
					String patientId = getSubjectPatientIdOrNull(entry.getResource());
					if (patientId != null) {
						out.get(patientIdTag).output(KV.of(patientId, 1));
					}
				}
			}
		}
	}
}
