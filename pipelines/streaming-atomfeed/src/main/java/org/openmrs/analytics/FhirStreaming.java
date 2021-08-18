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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
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
	
	private static final String FHIR_ENDPOINT = "/ws/fhir2/R4";
	
	private static String sourceUrl;
	
	private static String sourcePassword;
	
	private static String sourceUser;
	
	private static String sinkPath;
	
	private static String sinkUser;
	
	private static String sinkPassword;
	
	public static void main(String[] args) throws InterruptedException, URISyntaxException {
		
		StreamingArgs streamingArgs = StreamingArgs.getInstance();
		JCommander.newBuilder().addObject(streamingArgs).build().parse(args);
		
		sourceUrl = streamingArgs.openmrsServerUrl;
		sourceUser = streamingArgs.openmrUserName;
		sourcePassword = streamingArgs.openmrsPassword;
		sinkPath = streamingArgs.fhirSinkPath;
		sinkUser = streamingArgs.sinkUserName;
		sinkPassword = streamingArgs.sinkPassword;
		
		String feedUrl = sourceUrl + FEED_ENDPOINT;
		String fhirUrl = sourceUrl + FHIR_ENDPOINT;
		
		// We can load ApplicationContext from the openmrs dependency like this but there should be
		// an easier/more lightweight way of just using the AtomFeedClient which is all we need!
		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/applicationContext-service.xml");
		
		log.info("Started listening on the feed " + feedUrl);
		
		// TODO: Autowire
		FhirContext fhirContext = FhirContext.forR4();
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirUrl, sourceUser, sourcePassword, fhirContext);
		
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkPath, sinkUser, sinkPassword,
		    fhirContext.getRestfulClientFactory());
		
		FeedConsumer feedConsumer = new FeedConsumer(feedUrl, fhirStoreUtil, openmrsUtil);
		
		while (true) {
			feedConsumer.listen();
			Thread.sleep(3000);
		}
	}
	
	@Parameters(separators = "=")
	public static class StreamingArgs extends BaseArgs {
		
		private static StreamingArgs streamingArgs;
		
		private StreamingArgs() {
		};
		
		public synchronized static StreamingArgs getInstance() {
			if (streamingArgs == null) {
				streamingArgs = new StreamingArgs();
			}
			return streamingArgs;
		}
	}
}
