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
import ca.uhn.fhir.parser.IParser;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
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
	
	private String[] args;
	
	public DebeziumListener(String[] args) {
		this.args = args;
	}
	
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
		String fhirBaseUrl = getPassedArgs().openmrsServerUrl + getPassedArgs().openmrsfhirBaseEndpoint;
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirBaseUrl, getPassedArgs().openmrUserName,
		        getPassedArgs().openmrsPassword, fhirContext);
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(getPassedArgs().cloudGcpFhirStore, fhirContext);
		IParser parser = fhirContext.newJsonParser();
		ParquetUtil parquetUtil = new ParquetUtil(fhirContext);
		camelContext.addService(new ParquetService(parquetUtil), true);
		return new FhirConverter(openmrsUtil, parser, fhirStoreUtil, parquetUtil);
	}
	
	private String getDebeziumConfig() {
		return "debezium-mysql:" + getPassedArgs().databaseHostName + "?" + "databaseHostname="
		        + getPassedArgs().databaseHostName + "&databaseServerId=" + getPassedArgs().databaseServerId
		        + "&databasePort=" + getPassedArgs().databasePort.intValue() + "&databaseUser="
		        + getPassedArgs().databaseUser + "&databasePassword=" + getPassedArgs().databasePassword
				//+ "&name={{database.dbname}}"
		        + "&databaseServerName=" + getPassedArgs().databaseName + "&databaseWhitelist="
		        + getPassedArgs().databaseSchema + "&offsetStorage=org.apache.kafka.connect.storage.FileOffsetBackingStore"
		        + "&offsetStorageFileName=" + getPassedArgs().databaseOffsetStorage + "&databaseHistoryFileFilename="
		        + getPassedArgs().databaseHistory
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
	
	@Parameters(separators = "=")
	public static class DebeziumArgs extends BaseArgs {
		
		private static DebeziumArgs debeziumArgs;
		
		private DebeziumArgs() {
		};
		
		@Parameter(names = { "--databaseHostName" }, description = "Host name on which the source database runs")
		public String databaseHostName = "localhost";
		
		@Parameter(names = { "--databasePort" }, description = "Port of the source database")
		public Integer databasePort = 3306;
		
		@Parameter(names = { "--databaseUser" }, description = "User name of the host Database")
		public String databaseUser = "root";
		
		@Parameter(names = { "--databasePassword" }, description = "Passowrd for the user of the host Database")
		public String databasePassword = "root";
		
		@Parameter(names = { "--databaseName" }, description = "Name Database")
		public String databaseName = "mysql";
		
		@Parameter(names = { "--databaseSchema" }, description = "Name the Schema")
		public String databaseSchema = "openmrs";
		
		@Parameter(names = { "--databaseServerId" }, description = "Server Id of the source database")
		public Integer databaseServerId = 77;
		
		@Parameter(names = { "--databaseOffsetStorage" }, description = "Database OffsetStorage setting")
		public String databaseOffsetStorage = "data/offset.dat";
		
		@Parameter(names = { "--databaseHistory" }, description = "Database OffsetStorage setting")
		public String databaseHistory = "data/dbhistory.dat";
		
		@Parameter(names = { "--openmrsfhirBaseEndpoint" }, description = "Fhir base endpoint")
		public String openmrsfhirBaseEndpoint = "/openmrs/ws/fhir2/R3";
		
		@Parameter(names = { "--fileParquetPath" }, description = "Google cloud FHIRE store")
		public String fileParquetPath = "/tmp/";
		
		@Parameter(names = { "--fhirDebeziumEventConfigPath" }, description = "Google cloud FHIR store")
		public String fhirDebeziumEventConfigPath = "utils/dbz_event_to_fhir_config.json";
		
		public static DebeziumArgs getInstance() {
			if (debeziumArgs == null) {
				debeziumArgs = new DebeziumArgs();
			}
			return debeziumArgs;
		}
	}
	
	public DebeziumArgs getPassedArgs() {
		DebeziumArgs params = DebeziumArgs.getInstance();
		JCommander.newBuilder().addObject(params).build().parse(this.args);
		return params;
	}
	
}
