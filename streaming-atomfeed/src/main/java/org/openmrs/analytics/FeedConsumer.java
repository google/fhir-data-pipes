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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.hl7.fhir.r4.model.*;
import org.openmrs.module.atomfeed.client.AtomFeedClient;
import org.openmrs.module.atomfeed.client.AtomFeedClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeedConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(FeedConsumer.class);
	
	private List<AtomFeedClient> feedClients = new ArrayList<>();
	
	FeedConsumer(String feedUrl, FhirStoreUtil fhirStoreUtil, OpenmrsUtil openmrsUtil) throws URISyntaxException {
		// TODO what we really need is a list of pairs!
		Map<String, Class> categories = new LinkedHashMap<>();
		categories.put("Patient", Patient.class);
		categories.put("Encounter", Encounter.class);
		categories.put("Observation", Observation.class);
		categories.put("Location", Location.class);
		categories.put("Allergy", AllergyIntolerance.class);
		
		// TODO add other FHIR resources that are implemented in OpenMRS.
		for (Map.Entry<String, Class> entry : categories.entrySet()) {
			FhirEventWorker eventWorker = new FhirEventWorker(fhirStoreUtil, openmrsUtil);
			
			AtomFeedClient feedClient = AtomFeedClientFactory.createClient(eventWorker);
			
			// TODO check if this can be set by configuring above factory call & finalize the feed number.
			URI feedUri = new URI(feedUrl + "/" + entry.getKey().toLowerCase() + "/1");
			
			feedClient.setUri(feedUri);
			feedClients.add(feedClient);
		}
	}
	
	public void listen() {
		for (AtomFeedClient client : feedClients) {
			try {
				client.process();
			}
			catch (Exception e) {
				log.info("Failed to process client" + client.getUri() + " | " + e.getMessage());
			}
		}
	}
	
}
