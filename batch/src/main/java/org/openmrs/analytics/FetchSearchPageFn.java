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
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add unit-tests for this class and its sub-classes in PTransforms.

/**
 * This is the common functionality for all Fns that need to fetch FHIR resources and convert them
 * to Avro and JSON records. The non-abstract sub-classes should implement `ProcessElement` using
 * `processBundle` auxiliary method.
 *
 * @param <T> The type of the elements of the input PCollection.
 */
abstract class FetchSearchPageFn<T> extends DoFn<T, GenericRecord> {
	
	private static final Logger log = LoggerFactory.getLogger(FetchSearchPageFn.class);
	
	private final Counter numFetchedResources;
	
	private final Counter totalFetchTimeMillis;
	
	private final Counter totalGenerateTimeMillis;
	
	private final Counter totalPushTimeMillis;
	
	protected final TupleTag<GenericRecord> avroTag = new TupleTag<GenericRecord>() {};
	
	protected final TupleTag<String> jsonTag = new TupleTag<String>() {};
	
	private final String sourceUrl;
	
	private final String sourceUser;
	
	private final String sourcePw;
	
	private final String sinkPath;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	protected final String stageIdentifier;
	
	private final String parquetFile;
	
	private final String jsonFile;
	
	private ParquetUtil parquetUtil;
	
	protected OpenmrsUtil openmrsUtil;
	
	protected FhirSearchUtil fhirSearchUtil;
	
	private FhirStoreUtil fhirStoreUtil;
	
	private IParser parser;
	
	FetchSearchPageFn(FhirEtlOptions options, String stageIdentifier) {
		this.sinkPath = options.getFhirSinkPath();
		this.sinkUsername = options.getSinkUserName();
		this.sinkPassword = options.getSinkPassword();
		this.sourceUrl = options.getOpenmrsServerUrl() + options.getServerFhirEndpoint();
		this.sourceUser = options.getOpenmrsUserName();
		this.sourcePw = options.getOpenmrsPassword();
		this.stageIdentifier = stageIdentifier;
		this.parquetFile = options.getOutputParquetPath();
		this.jsonFile = options.getOutputJsonPath();
		this.numFetchedResources = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + stageIdentifier);
		this.totalFetchTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalFetchTimeMillis_" + stageIdentifier);
		this.totalGenerateTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE,
		    "totalGenerateTimeMillis_" + stageIdentifier);
		this.totalPushTimeMillis = Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + stageIdentifier);
	}
	
	@Setup
	public void Setup() {
		FhirContext fhirContext = FhirContext.forR4();
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		openmrsUtil = new OpenmrsUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
		fhirSearchUtil = new FhirSearchUtil(openmrsUtil);
		parquetUtil = new ParquetUtil(this.parquetFile);
		parser = fhirContext.newJsonParser();
	}
	
	protected void addFetchTime(long millis) {
		totalFetchTimeMillis.inc(millis);
	}
	
	protected void processBundle(Bundle bundle, MultiOutputReceiver out) {
		if (bundle != null && bundle.getEntry() != null) {
			numFetchedResources.inc(bundle.getEntry().size());
			if (!parquetFile.isEmpty()) {
				long startTime = System.currentTimeMillis();
				List<GenericRecord> recordList = parquetUtil.generateRecords(bundle);
				for (GenericRecord record : recordList) {
					try {
						out.get(avroTag).output(record);
					}
					catch (AvroTypeException e) {
						// TODO fix the root cause of this exception!
						log.error(String.format("Dropping record with id %s at stage %s due to exception: %s ",
						    record.get("id"), stageIdentifier, e));
					}
				}
				totalGenerateTimeMillis.inc(System.currentTimeMillis() - startTime);
			}
			if (!jsonFile.isEmpty()) {
				for (BundleEntryComponent entry : bundle.getEntry()) {
					out.get(jsonTag).output(parser.encodeResourceToString(entry.getResource()));
				}
			}
			if (!this.sinkPath.isEmpty()) {
				long pushStartTime = System.currentTimeMillis();
				fhirStoreUtil.uploadBundle(bundle);
				totalPushTimeMillis.inc(System.currentTimeMillis() - pushStartTime);
			}
		}
	}
	
	protected PCollection<GenericRecord> getAvroRecords(PCollectionTuple records) {
		return records.get(avroTag);
	}
	
	protected PCollection<String> getJsonRecords(PCollectionTuple records) {
		return records.get(jsonTag);
	}
	
}
