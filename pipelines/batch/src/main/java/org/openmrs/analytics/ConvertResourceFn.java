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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;

// TODO: A good amount of functionality/setup here is shared with FetchSearchPageFn. 
// There is room for refactoring in the future.
public class ConvertResourceFn extends DoFn<JdbcHapiRowDescriptor, Integer> {
	
	private final HashMap<String, Counter> numFetchedResources;
	
	private final HashMap<String, Counter> totalParseTimeMillis;
	
	private final HashMap<String, Counter> totalGenerateTimeMillis;
	
	private final HashMap<String, Counter> totalPushTimeMillis;
	
	private final String sinkPath;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	private final String parquetFile;
	
	private final int secondsToFlush;
	
	private final int rowGroupSize;
	
	@VisibleForTesting
	protected ParquetUtil parquetUtil;
	
	protected FhirStoreUtil fhirStoreUtil;
	
	private SimpleDateFormat simpleDateFormat;
	
	private IParser parser;
	
	ConvertResourceFn(FhirEtlOptions options) {
		this.sinkPath = options.getFhirSinkPath();
		this.sinkUsername = options.getSinkUserName();
		this.sinkPassword = options.getSinkPassword();
		this.parquetFile = options.getOutputParquetPath();
		this.secondsToFlush = options.getSecondsToFlushParquetFiles();
		this.rowGroupSize = options.getRowGroupSizeForParquetFiles();
		
		this.numFetchedResources = new HashMap<String, Counter>();
		this.totalParseTimeMillis = new HashMap<String, Counter>();
		this.totalGenerateTimeMillis = new HashMap<String, Counter>();
		this.totalPushTimeMillis = new HashMap<String, Counter>();
		List<String> resourceTypes = Arrays.asList(options.getResourceList().split(","));
		for (String resourceType : resourceTypes) {
			this.numFetchedResources.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + resourceType));
			this.totalParseTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalParseTimeMillis_" + resourceType));
			this.totalGenerateTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalGenerateTimeMillis_" + resourceType));
			this.totalPushTimeMillis.put(resourceType,
			    Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + resourceType));
		}
	}
	
	@Setup
	public void setup() {
		FhirContext fhirContext = FhirContext.forR4();
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUsername, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		parquetUtil = new ParquetUtil(parquetFile, secondsToFlush, rowGroupSize, "");
		parser = fhirContext.newJsonParser();
		simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	}
	
	@Teardown
	public void teardown() throws IOException {
		parquetUtil.closeAllWriters();
	}
	
	public void writeResource(JdbcHapiRowDescriptor element) throws IOException, ParseException {
		String resourceId = element.resourceId();
		String resourceType = element.resourceType();
		Meta meta = new Meta().setVersionId(element.resourceVersion())
		        .setLastUpdated(simpleDateFormat.parse(element.lastUpdated()));
		String jsonResource = element.jsonResource();
		
		long startTime = System.currentTimeMillis();
		Resource resource = (Resource) parser.parseResource(jsonResource);
		totalParseTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		resource.setId(resourceId);
		resource.setMeta(meta);
		
		numFetchedResources.get(resourceType).inc(1);
		
		if (!parquetFile.isEmpty()) {
			startTime = System.currentTimeMillis();
			parquetUtil.write(resource);
			totalGenerateTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		}
		if (!this.sinkPath.isEmpty()) {
			startTime = System.currentTimeMillis();
			fhirStoreUtil.uploadResource(resource);
			totalPushTimeMillis.get(resourceType).inc(System.currentTimeMillis() - startTime);
		}
	}
	
	@ProcessElement
	public void processElement(ProcessContext processContext) throws IOException, ParseException {
		JdbcHapiRowDescriptor element = processContext.element();
		writeResource(element);
	}
}
