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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import ca.uhn.fhir.parser.IParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.debezium.data.Envelope.Operation;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.hl7.fhir.dstu3.model.Resource;
import org.openmrs.module.atomfeed.api.model.FeedConfiguration;
import org.openmrs.module.atomfeed.api.model.GeneralConfiguration;
import org.openmrs.module.atomfeed.api.service.FeedConfigurationService;
import org.openmrs.module.atomfeed.api.service.impl.FeedConfigurationServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A Debezium events to FHIR URI mapper
public class FhirConverter implements Processor {
	
	private static final Logger log = LoggerFactory.getLogger(FhirConverter.class);
	
	private final FeedConfigurationService feedConfigurationService = new FeedConfigurationServiceImpl();
	
	private final OpenmrsUtil openmrsUtil;
	
	private final FhirStoreUtil fhirStoreUtil;
	
	private final IParser parser;
	
	@VisibleForTesting
	FhirConverter() {
		this.openmrsUtil = null;
		this.fhirStoreUtil = null;
		this.parser = null;
	}
	
	public FhirConverter(OpenmrsUtil openmrsUtil, FhirStoreUtil fhirStoreUtil, IParser parser) throws IOException {
		// TODO add option for switching to Parquet-file outputs.
		this.openmrsUtil = openmrsUtil;
		this.fhirStoreUtil = fhirStoreUtil;
		this.parser = parser;
		
		GeneralConfiguration generalConfiguration = getEventsToFhirConfig(
		    System.getProperty("fhir.debeziumEventConfigPath"));
		this.feedConfigurationService.saveConfig(generalConfiguration);
	}
	
	public void process(Exchange exchange) {
		Message message = exchange.getMessage();
		final Map payload = message.getBody(Map.class);
		final Map sourceMetadata = message.getHeader(DebeziumConstants.HEADER_SOURCE_METADATA, Map.class);
		String operation = message.getHeader(DebeziumConstants.HEADER_OPERATION, String.class);
		// for malformed event, return null
		if (sourceMetadata == null || payload == null || operation == null) {
			return;
		}
		if (!operation.equals(Operation.CREATE.code()) && !operation.equals(Operation.UPDATE.code())) {
			log.debug("Skipping non create/update operation " + message.getHeaders());
			return;
		}
		final String table = sourceMetadata.get("table").toString();
		log.debug("Processing Table --> " + table);
		final FeedConfiguration config = this.feedConfigurationService.getFeedConfigurationByCategory(table);
		if (config == null || !config.getLinkTemplates().containsKey("fhir") || !payload.containsKey("uuid")) {
			log.trace("Skipping unmapped data ..." + table);
			return;
		}
		final String uuid = payload.get("uuid").toString();
		final String fhirUrl = config.getLinkTemplates().get("fhir").replace("{uuid}", uuid);
		log.info("Fetching FHIR resource at " + fhirUrl);
		Resource resource = openmrsUtil.fetchFhirResource(fhirUrl);
		if (resource == null) {
			// TODO: check how this can be signalled to Camel to be retried.
			return;
		}
		String resourceJson = parser.encodeResourceToString(resource);
		fhirStoreUtil.uploadResourceToCloud(resource.fhirType(), resource.getId(), resourceJson);
	}
	
	@VisibleForTesting
	GeneralConfiguration getEventsToFhirConfig(String fileName) throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(fileName);
		try (Reader reader = Files.newBufferedReader(pathToFile.toAbsolutePath(), StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfig = gson.fromJson(reader, GeneralConfiguration.class);
			return generalConfig;
		}
	}
}
