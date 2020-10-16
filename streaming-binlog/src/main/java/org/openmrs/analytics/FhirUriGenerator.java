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

import static org.openmrs.analytics.PipelineConfig.getEventsToFhirConfig;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.openmrs.module.atomfeed.api.model.FeedConfiguration;
import org.openmrs.module.atomfeed.api.model.GeneralConfiguration;
import org.openmrs.module.atomfeed.api.service.FeedConfigurationService;
import org.openmrs.module.atomfeed.api.service.impl.FeedConfigurationServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A Debezium events to FHIR URI mapper
public class FhirUriGenerator implements Processor {
	
	private static final Logger log = LoggerFactory.getLogger(FhirUriGenerator.class);
	
	private static final String EXPECTED_BODY_FORMAT = "{\"fhirResourceUri\":%s}";
	
	private final FeedConfigurationService feedConfigurationService = new FeedConfigurationServiceImpl();
	
	public FhirUriGenerator() throws Exception {
		//System.getProperty("cloud.gcpFhirStore")
		GeneralConfiguration generalConfiguration = getEventsToFhirConfig(
		    System.getProperty("fhir.debeziumEventConfigPath"));
		this.feedConfigurationService.saveConfig(generalConfiguration);
	}
	
	public void process(Exchange exchange) throws Exception {
		final Map payload = exchange.getMessage().getBody(Map.class);
		final Map sourceMetadata = exchange.getMessage().getHeader(DebeziumConstants.HEADER_SOURCE_METADATA, Map.class);
		// for malformed event, return null
		if (sourceMetadata == null || payload == null) {
			exchange.getIn().setHeader("fhirResourceUri", null);
			return;
		}
		final String table = sourceMetadata.get("table").toString();
		log.info("Processing Table --> " + table);
		final FeedConfiguration config = this.feedConfigurationService.getFeedConfigurationByCategory(table);
		if (config != null && config.getLinkTemplates().containsKey("fhir")) {
			final String fhirUrl = config.getLinkTemplates().get("fhir");
			final String uuid = payload.get("uuid").toString();
			exchange.getIn().setHeader("fhirResourceUri", fhirUrl.replace("{uuid}", uuid));
		} else {
			exchange.getIn().setHeader("fhirResourceUri", null);
			log.trace("Unmapped Data..." + table);
		}
	}
}
