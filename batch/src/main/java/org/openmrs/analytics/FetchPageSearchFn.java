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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.SummaryEnum;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function object fetches all input segments from the FHIR source and converts each resource
 * to JSON or Avro (or both). If a FHIR sink address is provided, those resources are passed to that
 * FHIR sink too.
 */
// TODO: Add unit-tests for this class.
class FetchSearchPageFn extends DoFn<SearchSegmentDescriptor, GenericRecord> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchSearchPageFn.class);
	
	private final Counter numFetchedResources;
	
	private final Counter totalFetchTimeMillis;
	
	private final Counter totalGenerateTimeMillis;
	
	private final Counter totalPushTimeMillis;
	
	final TupleTag<GenericRecord> avroTag = new TupleTag<GenericRecord>() {};
	
	final TupleTag<String> jsonTag = new TupleTag<String>() {};
	
	private final String sourceUrl;
	
	private final String sourceUser;
	
	private final String sourcePw;
	
	private final String sinkPath;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	private final String resourceType;
	
	private final String parquetFile;
	
	private final String jsonFile;
	
	private ParquetUtil parquetUtil;
	
	private FhirSearchUtil fhirSearchUtil;
	
	private FhirStoreUtil fhirStoreUtil;
	
	private IParser parser;
	
	FetchSearchPageFn(FhirEtlOptions options, String resourceType) {
		this.sinkPath = options.getFhirSinkPath();
		this.sinkUsername = options.getSinkUserName();
		this.sinkPassword = options.getSinkPassword();
		this.sourceUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint();
		this.sourceUser = options.getOpenmrsUserName();
		this.sourcePw = options.getOpenmrsPassword();
		this.resourceType = resourceType;
		this.parquetFile = options.getOutputParquetPath();
		this.jsonFile = options.getOutputJsonPath();
		this.numFetchedResources = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + resourceType);
		this.totalFetchTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalFetchTimeMillis_" + resourceType);
		this.totalGenerateTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE,
		    "totalGenerateTimeMillis_" + resourceType);
		this.totalPushTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + resourceType);
	}
	
	@Setup
	public void Setup() {
		FhirContext fhirContext = FhirContext.forR4();
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		fhirSearchUtil = new FhirSearchUtil(new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext));
		parquetUtil = new ParquetUtil(this.parquetFile);
		parser = fhirContext.newJsonParser();
	}
	
	@ProcessElement
	public void ProcessElement(@Element SearchSegmentDescriptor segment, MultiOutputReceiver out) {
		String searchUrl = segment.searchUrl();
		log.info(String.format("Fetching %d %s resources: %s", segment.count(), this.resourceType,
		    searchUrl.substring(0, Math.min(200, searchUrl.length()))));
		long fetchStartTime = System.currentTimeMillis();
		Bundle pageBundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), SummaryEnum.DATA);
		totalFetchTimeMillis.inc(System.currentTimeMillis() - fetchStartTime);
		if (pageBundle != null && pageBundle.getEntry() != null) {
			numFetchedResources.inc(pageBundle.getEntry().size());
			if (!parquetFile.isEmpty()) {
				long startTime = System.currentTimeMillis();
				List<GenericRecord> recordList = parquetUtil.generateRecords(pageBundle);
				for (GenericRecord record : recordList) {
					out.get(avroTag).output(record);
				}
				totalGenerateTimeMillis.inc(System.currentTimeMillis() - startTime);
			}
			if (!jsonFile.isEmpty()) {
				for (BundleEntryComponent entry : pageBundle.getEntry()) {
					out.get(jsonTag).output(parser.encodeResourceToString(entry.getResource()));
				}
			}
		}
		if (!this.sinkPath.isEmpty()) {
			long pushStartTime = System.currentTimeMillis();
			fhirStoreUtil.uploadBundle(pageBundle);
			totalPushTimeMillis.inc(System.currentTimeMillis() - pushStartTime);
		}
	}
}
