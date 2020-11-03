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

import java.net.URISyntaxException;

import ca.uhn.fhir.context.FhirContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A standalone app that listens on Atom Feeds of an OpenMRS server and translates the changes in
 * OpenMRS to FHIR resources that are exported to GCP FHIR store.
 */
public class FhirStreaming {
	
	private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);
	
	// TODO: set as arg or env variable? using constant for simplicity
	private static final String FEED_ENDPOINT = "/ws/atomfeed";
	
	private static final String FHIR_ENDPOINT = "/ws/fhir2/R3";
	
	private static String sourceUrl;
	
	private static String sourcePassword;
	
	private static String sourceUser;
	
	private static String sinkPath;
	
	public static void main(String[] args) throws InterruptedException, URISyntaxException {
		if (args.length == 3) {
			sourceUrl = args[0];
			
			sourceUser = args[1].split("/")[0];
			sourcePassword = args[1].split("/")[1];
			sinkPath = args[2];
		} else {
			log.error("You should pass the following arguements:");
			log.error("1) source url: the base url of the OpenMRS server (ending in 'openmrs').");
			log.error("2) source auth user / password.");
			log.error("3) a GCP FHIR store in the following format:\n"
			        + "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIR_STORE \n"
			        + "where the all-caps parts should be updated based on your FHIR store, e.g., \n"
			        + "projects/my-project/locations/us-central1/datasets/my_dataset/fhirStores/test");
			log.error("Note it is expected that a MySQL DB with name `atomfeed_client` \n"
			        + "exists (configurable in `hibernate.default.properties`) with tables \n"
			        + "'failed_events' and 'markers' to track feed progress. \n"
			        + "Use utils/create_db.sql to create these. \n");
			return;
		}
		
		String feedUrl = sourceUrl + FEED_ENDPOINT;
		String fhirUrl = sourceUrl + FHIR_ENDPOINT;
		
		// We can load ApplicationContext from the openmrs dependency like this but there should be
		// an easier/more lightweight way of just using the AtomFeedClient which is all we need!
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/applicationContext-service.xml");
		
		log.info("Started listening on the feed " + feedUrl);
		
		// TODO: Autowire
		FhirContext fhirContext = FhirContext.forDstu3();
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirUrl, sourceUser, sourcePassword, fhirContext);
		
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, fhirContext.getRestfulClientFactory());
		
		FeedConsumer feedConsumer = new FeedConsumer(feedUrl, fhirStoreUtil, openmrsUtil);
		
		while (true) {
			feedConsumer.listen();
			Thread.sleep(3000);
		}
	}
}
