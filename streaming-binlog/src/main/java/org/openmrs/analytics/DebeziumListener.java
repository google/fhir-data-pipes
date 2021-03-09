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

import ca.uhn.fhir.context.FhirContext;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
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
	
	private GeneralConfiguration generalConfiguration;
	
	private DebeziumArgs params;
	
	public DebeziumListener(String[] args) throws IOException {
		this.params = new DebeziumArgs();
		JCommander.newBuilder().addObject(params).build().parse(args);
		this.generalConfiguration = getFhirDebeziumConfigPath(params.fhirDebeziumConfigPath);
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
	FhirConverter createFhirConverter(CamelContext camelContext) throws Exception {
		FhirContext fhirContext = FhirContext.forR4();
		String fhirBaseUrl = params.openmrsServerUrl + params.openmrsfhirBaseEndpoint;
		OpenmrsUtil openmrsUtil = new OpenmrsUtil(fhirBaseUrl, params.openmrUserName, params.openmrsPassword, fhirContext);
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(params.fhirSinkPath, params.sinkUserName,
		    params.sinkPassword, fhirContext.getRestfulClientFactory());
		ParquetUtil parquetUtil = new ParquetUtil(params.outputParquetPath, params.secondsToFlushParquetFiles,
		        params.rowGroupSizeForParquetFiles);
		JdbcConnectionUtil jdbcConnectionUtil = new JdbcConnectionUtil(params.jdbcDriverClass, params.jdbcUrlInput,
		        this.generalConfiguration.getDebeziumConfigurations().get("databaseUser"),
		        this.generalConfiguration.getDebeziumConfigurations().get("databasePassword"), params.initialPoolSize,
		        params.jdbcMaxPoolSize);
		UuidUtil uuidUtil = new UuidUtil(jdbcConnectionUtil);
		camelContext.addService(new ParquetService(parquetUtil), true);
		return new FhirConverter(openmrsUtil, fhirStoreUtil, parquetUtil, params.fhirDebeziumConfigPath, uuidUtil);
	}
	
	private String getDebeziumConfig() {
		Map<String, String> debeziumConfigs = this.generalConfiguration.getDebeziumConfigurations();
		return "debezium-mysql:" + debeziumConfigs.get("databaseServerName") + "?" + "databaseHostname="
		        + debeziumConfigs.get("databaseHostName") + "&databaseServerId=" + debeziumConfigs.get("databaseServerId")
		        + "&databasePort=" + debeziumConfigs.get("databasePort") + "&databaseUser="
		        + debeziumConfigs.get("databaseUser") + "&databasePassword=" + debeziumConfigs.get("databasePassword")
		        + "&databaseServerName=" + debeziumConfigs.get("databaseServerName") + "&databaseWhitelist="
		        + debeziumConfigs.get("databaseSchema")
		        + "&offsetStorage=org.apache.kafka.connect.storage.FileOffsetBackingStore" + "&offsetStorageFileName="
		        + debeziumConfigs.get("databaseOffsetStorage") + "&databaseHistoryFileFilename="
		        + debeziumConfigs.get("databaseHistory") + "&snapshotMode=" + debeziumConfigs.get("snapshotMode");
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
		
		@Parameter(names = { "--openmrsfhirBaseEndpoint" }, description = "Fhir base endpoint")
		public String openmrsfhirBaseEndpoint = "/ws/fhir2/R4";
		
		@Parameter(names = { "--fhirDebeziumConfigPath" }, description = "Google cloud FHIR store")
		public String fhirDebeziumConfigPath = "utils/dbz_event_to_fhir_config.json";
		
		@Parameter(names = { "--jdbcDriverClass" }, description = "JDBC MySQL driver class")
		public String jdbcDriverClass = "com.mysql.cj.jdbc.Driver";
		
		@Parameter(names = { "--jdbcUrl" }, description = "JDBC URL input")
		public String jdbcUrlInput = "jdbc:mysql://localhost:3306/openmrs";
		
		@Parameter(names = { "--jdbcMaxPoolSize" }, description = "JDBC maximum pool size")
		public int jdbcMaxPoolSize = 50;
		
		@Parameter(names = { "--jdbcInitialPoolSize" }, description = "JDBC initial pool size")
		public int initialPoolSize = 10;
	}
	
	private GeneralConfiguration getFhirDebeziumConfigPath(String fileName) throws IOException {
		Gson gson = new Gson();
		Path pathToFile = Paths.get(fileName);
		try (Reader reader = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
			GeneralConfiguration generalConfiguration = gson.fromJson(reader, GeneralConfiguration.class);
			return generalConfiguration;
		}
	}
}
