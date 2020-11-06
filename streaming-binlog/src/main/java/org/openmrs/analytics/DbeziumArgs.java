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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class DbeziumArgs {
	
	@Parameter(names = { "--databaseHostName" }, description = "Host name on which the source database runs")
	public String databaseHostName = "localhost";
	
	@Parameter(names = { "--databasePort" }, description = "Port of the source database")
	public Integer databasePort = 3306;
	
	@Parameter(names = { "--databaseUser" }, description = "user name of the host Database")
	public String databaseUser = "root";
	
	@Parameter(names = { "--databasePassword" }, description = "passowrd for the user of the host Database")
	public String databasePassword = "root";
	
	@Parameter(names = { "--databaseName" }, description = "Name Database")
	public String databaseName = "mysql";
	
	@Parameter(names = { "--databaseSchema" }, description = "Name the Schema")
	public String databaseSchema = "openmrs";
	
	@Parameter(names = { "--databaseServerId" }, description = "Server Id of the source database")
	public Integer databaseServerId = 77;
	
	@Parameter(names = { "--databaseOffsetStorage" }, description = "database OffsetStorage setting")
	public String databaseOffsetStorage = "data/offset.dat";
	
	@Parameter(names = { "--databaseHistory" }, description = "database OffsetStorage setting")
	public String databaseHistory = "data/dbhistory.dat";
	
	@Parameter(names = { "--openmrUserName" }, description = "user name for openmrs server")
	public String openmrUserName = "admin";
	
	@Parameter(names = { "--openmrsPassword" }, description = "Password for openmrs User")
	public String openmrsPassword = "admin";
	
	@Parameter(names = { "--openmrsServerUrl" }, description = "openmrs Server Base Url")
	public String openmrsServerUrl = "http://localhost:8099";
	
	@Parameter(names = { "--openmrsfhirBaseEndpoint" }, description = "Fhir base endpoint")
	public String openmrsfhirBaseEndpoint = "/openmrs/ws/fhir2/R3";
	
	@Parameter(names = { "--cloudGcpFhirStore" }, description = "Google cloud FHIRE store")
	public String cloudGcpFhirStore = "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME";
	
	@Parameter(names = { "--fileParquetPath" }, description = "Google cloud FHIRE store")
	public String fileParquetPath = "/tmp/";
	
	@Parameter(names = { "--fhirDebeziumEventConfigPath" }, description = "Google cloud FHIRE store")
	public String fhirDebeziumEventConfigPath = "utils/dbz_event_to_fhir_config.json";
}
