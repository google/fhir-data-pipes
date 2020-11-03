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

import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.hl7.fhir.dstu3.model.BaseResource;
import org.hl7.fhir.dstu3.model.Resource;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker<T extends BaseResource> implements EventWorker {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);
	
	private final FhirStoreUtil fhirStoreUtil;
	
	private final OpenmrsUtil openmrsUtil;
	
	public FhirEventWorker(FhirStoreUtil fhirStoreUtil, OpenmrsUtil openmrsUtil) {
		this.fhirStoreUtil = fhirStoreUtil;
		this.openmrsUtil = openmrsUtil;
	}
	
	@Override
	public void process(Event event) {
		String content = event.getContent();
		if (content == null || "".equals(content)) {
			log.warn("No content in event: " + event);
			return;
		}
		try {
			JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
			String fhirUrl = jsonObj.get("fhir").getAsString();
			if (fhirUrl == null || "".equals(fhirUrl)) {
				log.info("Skipping non-FHIR event " + event);
				return;
			}
			
			log.info("FHIR resource URL is: " + fhirUrl);
			T resource = (T) openmrsUtil.fetchFhirResource(fhirUrl);
			String resourceId = resource.getIdElement().getIdPart();
			String resourceType = resource.getIdElement().getResourceType();
			
			log.info(String.format("Parsed FHIR resource ID is %s/%s", resourceType, resourceId));
			
			fhirStoreUtil.uploadResourceToCloud((Resource) resource);
		}
		catch (JsonParseException e) {
			log.error(String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
		}
	}
	
	@Override
	public void cleanUp(Event event) {
		log.info("In clean-up!");
	}
	
}
