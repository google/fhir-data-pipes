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

import ca.uhn.fhir.context.FhirContext;
import com.google.common.annotations.VisibleForTesting;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Service;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Debezium change data capture / Listener
public class DebeziumListener extends RouteBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumListener.class);
	
	@VisibleForTesting
	final static String DEBEZIUM_ROUTE_ID = DebeziumListener.class.getName() + ".MysqlDatabaseCDC";
	
	@Override
	public void configure() throws IOException, Exception {
		log.info("Debezium Listener Started... ");
		
		// Main Change Data Capture (DBZ) entrypoint
		from(getDebeziumConfig()).routeId(DEBEZIUM_ROUTE_ID)
		        .log(LoggingLevel.TRACE, "Incoming Events: ${body} with headers ${headers}")
		        .process(createFhirConverter(getContext()));
	}
	
	@VisibleForTesting
	FhirConverter createFhirConverter(CamelContext camelContext) throws IOException, Exception {
		FhirContext fhirContext = FhirContext.forDstu3();
		String fhirBaseUrl = System.getProperty("openmrs.serverUrl") + System.getProperty("openmrs.fhirBaseEndpoint");
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirBaseUrl, System.getProperty("openmrs.username"),
		        System.getProperty("openmrs.password"), fhirContext);
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(System.getProperty("fhir.sinkPath"),
		    System.getProperty("fhir.sinkUsername"), System.getProperty("fhir.sinkPassword"),
		    fhirContext.getRestfulClientFactory());
		ParquetUtil parquetUtil = new ParquetUtil(fhirContext);
		camelContext.addService(new ParquetService(parquetUtil), true);
		return new FhirConverter(openmrsUtil, fhirStoreUtil, parquetUtil);
	}
	
	private String getDebeziumConfig() {
		return "debezium-mysql:{{database.hostname}}?" + "databaseHostname={{database.hostname}}"
		        + "&databaseServerId={{database.serverId}}" + "&databasePort={{database.port}}"
		        + "&databaseUser={{database.user}}" + "&databasePassword={{database.password}}"
				//+ "&name={{database.dbname}}"
		        + "&databaseServerName={{database.dbname}}" + "&databaseWhitelist={{database.schema}}"
		        + "&offsetStorage=org.apache.kafka.connect.storage.FileOffsetBackingStore"
		        + "&offsetStorageFileName={{database.offsetStorage}}"
		        + "&databaseHistoryFileFilename={{database.databaseHistory}}"
		//+ "&tableWhitelist={{database.schema}}.encounter,{{database.schema}}.obs"
		;
	}
	
	/**
	 * The only purpose for this service is to properly close ParquetWriter objects when the pipeline is
	 * stopped.
	 */
	private static class ParquetService implements Service {
		
		private ParquetUtil parquetUtil;
		
		ParquetService(ParquetUtil parquetUtil) {
			this.parquetUtil = parquetUtil;
		}
		
		@Override
		public void start() {
		}
		
		@Override
		public void stop() {
			parquetUtil.closeAllWriters();
		}
		
		@Override
		public void close() {
			stop();
		}
		
	}
	
}
