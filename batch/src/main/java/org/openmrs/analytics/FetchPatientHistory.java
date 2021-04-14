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

import ca.uhn.fhir.rest.api.SummaryEnum;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchPatientHistory extends PTransform<PCollection<KV<String, Integer>>, PCollectionTuple> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchPatientHistory.class);
	
	private final FetchSearchPageFn<KV<String, Integer>> fetchSearchPageFn;
	
	private final Schema schema;
	
	FetchPatientHistory(FhirEtlOptions options, String resourceType, Schema schema) {
		Preconditions.checkState(!options.getActivePeriod().isEmpty());
		List<String> dateRange = FhirSearchUtil.getDateRange(options.getActivePeriod());
		
		int count = options.getBatchSize();
		this.schema = schema;
		
		fetchSearchPageFn = new FetchSearchPageFn<KV<String, Integer>>(options, resourceType + "_history") {
			
			@ProcessElement
			public void ProcessElement(@Element KV<String, Integer> patientIdCount, MultiOutputReceiver out) {
				String patientId = patientIdCount.getKey();
				log.info(String.format("Fetching historical %s resources for patient  %s", resourceType, patientId));
				Bundle bundle = this.fhirSearchUtil.searchByPatientAndLastDate(resourceType, patientId, dateRange.get(0),
				    count);
				processBundle(bundle, out);
				String nextUrl = this.fhirSearchUtil.getNextUrl(bundle);
				while (nextUrl != null) {
					bundle = this.fhirSearchUtil.searchByUrl(nextUrl, count, SummaryEnum.DATA);
					processBundle(bundle, out);
					nextUrl = this.fhirSearchUtil.getNextUrl(bundle);
				}
			}
		};
	}
	
	@Override
	public PCollectionTuple expand(PCollection<KV<String, Integer>> patientIdCounts) {
		PCollectionTuple records = patientIdCounts.apply(ParDo.of(fetchSearchPageFn)
		        .withOutputTags(fetchSearchPageFn.avroTag, TupleTagList.of(Lists.newArrayList(fetchSearchPageFn.jsonTag))));
		records.get(fetchSearchPageFn.avroTag).setCoder(AvroCoder.of(GenericRecord.class, schema));
		return records;
	}
	
	public PCollection<GenericRecord> getAvroRecords(PCollectionTuple records) {
		return fetchSearchPageFn.getAvroRecords(records);
	}
	
	public PCollection<String> getJsonRecords(PCollectionTuple records) {
		return fetchSearchPageFn.getJsonRecords(records);
	}
	
}
