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

import java.util.Map;

import ca.uhn.fhir.context.FhirContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A processor that sinks FHIR cloud and local
// TODO implement sink to local
public class PipelineSink implements Processor {
	
	private static final Logger log = LoggerFactory.getLogger(PipelineSink.class);
	
	// TODO: Autowire
	// FhirContext
	private static final FhirContext fhirContext = FhirContext.forDstu3();
	
	// FhirStore
	private static final FhirStoreUtil FHIR_STORE_UTIL = new FhirStoreUtil(System.getProperty("cloud.gcpFhirStore"),
	        fhirContext);
	
	// Send to cloud TODO implement sink to local
	public void process(Exchange exchange) throws Exception {
		final Map kv = exchange.getMessage().getBody(Map.class);
		String resourceType = kv.get("resourceType").toString();
		String id = kv.get("id").toString();
		String fhirJson = exchange.getMessage().getBody(String.class);
		log.info("Sinking FHIR to Cloud ----> " + kv.get("resourceType") + "/" + kv.get("id"));
		// TODO read in a HAPI structures resource instead of raw json
		FHIR_STORE_UTIL.uploadResourceToCloud(resourceType, id, fhirJson);
	}
}
