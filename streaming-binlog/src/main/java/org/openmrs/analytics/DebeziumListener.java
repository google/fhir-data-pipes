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
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import org.apache.camel.CamelContext;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Service;
import org.apache.camel.builder.RouteBuilder;
import org.openmrs.analytics.model.GeneralConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Debezium change data capture / Listener
public class DebeziumListener extends RouteBuilder {
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumListener.class);
	
	private DebeziumArgs params;
	
	public DebeziumListener(String[] args) {
		this.params = new DebeziumArgs();
		JCommander.newBuilder().addObject(params).build().parse(args);
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
		FhirContext fhirContext = FhirContext.forR4();
		String fhirBaseUrl = params.openmrsServerUrl + params.openmrsfhirBaseEndpoint;
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirBaseUrl, params.openmrUserName, params.openmrsPassword, fhirContext);
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(params.fhirSinkPath, params.sinkUserName,
		    params.sinkPassword, fhirContext.getRestfulClientFactory());
		ParquetUtil parquetUtil = new ParquetUtil(params.fileParquetPath, params.secondsToFlushParquetFiles,
		        params.rowGroupSizeForParquetFiles);
		camelContext.addService(new ParquetService(parquetUtil), true);
		return new FhirConverter(openmrsUtil, fhirStoreUtil, parquetUtil, params.fhirDebeziumConfigPath);
	}
	
	private String getDebeziumConfig() throws IOException {
		GeneralConfiguration gc = new GeneralConfiguration();
		GeneralConfiguration generalConfiguration = gc.getFhirDebeziumConfigPath(params.fhirDebeziumConfigPath);
		return "debezium-mysql:" + generalConfiguration.getDebeziumConfigurations().get("databaseHostName") + "?"
		        + "databaseHostname=" + generalConfiguration.getDebeziumConfigurations().get("databaseHostName")
		        + "&databaseServerId=" + generalConfiguration.getDebeziumConfigurations().get("databaseServerId")
		        + "&databasePort=" + generalConfiguration.getDebeziumConfigurations().get("databasePort") + "&databaseUser="
		        + generalConfiguration.getDebeziumConfigurations().get("databaseUser") + "&databasePassword="
		        + generalConfiguration.getDebeziumConfigurations().get("databasePassword") + "&databaseServerName="
		        + generalConfiguration.getDebeziumConfigurations().get("databaseName") + "&databaseWhitelist="
		        + generalConfiguration.getDebeziumConfigurations().get("databaseSchema")
		        + "org.apache.kafka.connect.storage.FileOffsetBackingStore" + "&offsetStorageFileName="
		        + generalConfiguration.getDebeziumConfigurations().get("databaseOffsetStorage")
		        + "&databaseHistoryFileFilename=" + generalConfiguration.getDebeziumConfigurations().get("databaseHistory")
		        + "&snapshotMode=" + generalConfiguration.getDebeziumConfigurations().get("snapshotMode");
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
			try {
				parquetUtil.closeAllWriters();
			}
			catch (IOException e) {
				log.error("Could not close Parquet file writers properly!");
			}
		}
		
		@Override
		public void close() {
			stop();
		}
		
	}
	
	@Parameters(separators = "=")
	static class DebeziumArgs extends BaseArgs {
		
		public DebeziumArgs() {
		};
		
		@Parameter(names = { "--databaseHostName" }, description = "Host name on which the source database runs")
		public String databaseHostName = "localhost";
		
		@Parameter(names = { "--databasePort" }, description = "Port of the source database")
		public Integer databasePort = 3306;
		
		@Parameter(names = { "--databaseUser" }, description = "User name of the host Database")
		public String databaseUser = "root";
		
		@Parameter(names = { "--databasePassword" }, description = "Passowrd for the user of the host Database")
		public String databasePassword = "debezium";
		
		@Parameter(names = { "--databaseName" }, description = "Name Database")
		public String databaseName = "mysql";
		
		@Parameter(names = { "--databaseSchema" }, description = "Name the Schema")
		public String databaseSchema = "openmrs";
		
		@Parameter(names = { "--databaseServerId" }, description = "Server Id of the source database")
		public Integer databaseServerId = 223344;
		
		@Parameter(names = { "--databaseOffsetStorage" }, description = "Database OffsetStorage setting")
		public String databaseOffsetStorage = "data/offset.dat";
		
		@Parameter(names = { "--databaseHistory" }, description = "replacing Offset by History")
		public String databaseHistory = "data/dbhistory.dat";
		
		@Parameter(names = { "--snapshotMode" }, description = "criteria for running DB snapshot, options include: "
		        + "when_needed, schema_only, initial, never, e.t.c. To read events from day 0 of db data-entry, use 'initial'. "
		        + "To read only new events, use 'schema_only' ")
		public String snapshotMode = "initial";
		
		@Parameter(names = { "--openmrsfhirBaseEndpoint" }, description = "Fhir base endpoint")
		public String openmrsfhirBaseEndpoint = "/ws/fhir2/R4";
		
		@Parameter(names = { "--fhirDebeziumConfigPath" }, description = "Google cloud FHIR store")
		public String fhirDebeziumConfigPath = "utils/dbz_event_to_fhir_config.json";
		
	}
}
