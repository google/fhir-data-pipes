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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Options supported by {@link FhirEtl}.
 */
public interface FhirEtlOptions extends PipelineOptions {
	
	/**
	 * By default, this reads from the OpenMRS instance `openmrs` at the default port on localhost.
	 */
	@Description("Fhir source server URL")
	@Required
	@Default.String("http://localhost:8099/openmrs/ws/fhir2/R4")
	String getFhirServerUrl();
	
	void setFhirServerUrl(String value);
	
	@Description("Comma separated list of resource to fetch, e.g., 'Patient,Encounter,Observation'.")
	@Default.String("Patient,Encounter,Observation")
	String getResourceList();
	
	void setResourceList(String value);
	
	@Description("The number of resources to be fetched in one API call. "
	        + "For the JDBC mode passing > 170 could result in HTTP 400 Bad Request. "
	        + "Note by default the maximum bundle size for OpenMRS FHIR module is 100.")
	@Default.Integer(100)
	int getBatchSize();
	
	void setBatchSize(int value);
	
	@Description("For the JDBC mode, this is the size of each ID chunk. Setting high values will yield faster query "
	        + "execution.")
	@Default.Integer(10000)
	int getJdbcFetchSize();
	
	void setJdbcFetchSize(int value);
	
	@Description("Fhir source server BasicAuth Username")
	@Default.String("admin")
	String getFhirServerUserName();
	
	void setFhirServerUserName(String value);
	
	@Description("Fhir source server BasicAuth Password")
	@Default.String("Admin123")
	String getFhirServerPassword();
	
	void setFhirServerPassword(String value);
	
	@Description("The path to the target generic fhir store, or a GCP fhir store with the format: "
	        + "`projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
	        + "`projects/my-project/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test`")
	@Required
	@Default.String("")
	String getFhirSinkPath();
	
	void setFhirSinkPath(String value);
	
	@Description("Sink BasicAuth Username")
	@Default.String("")
	String getSinkUserName();
	
	void setSinkUserName(String value);
	
	@Description("Sink BasicAuth Password")
	@Default.String("")
	String getSinkPassword();
	
	void setSinkPassword(String value);
	
	@Description("The base name for output Parquet files; for each resource, one fileset will be created.")
	@Default.String("")
	String getOutputParquetPath();
	
	void setOutputParquetPath(String value);
	
	/**
	 * JDBC DB settings: defaults values have been pointed to ./openmrs-compose.yaml
	 */
	
	@Description("JDBC MySQL driver class")
	@Default.String("com.mysql.cj.jdbc.Driver")
	String getJdbcDriverClass();
	
	void setJdbcDriverClass(String value);
	
	@Description("JDBC maximum pool size")
	@Default.Integer(50)
	int getJdbcMaxPoolSize();
	
	void setJdbcMaxPoolSize(int value);
	
	@Description("JDBC initial pool size")
	@Default.Integer(10)
	int getJdbcInitialPoolSize();
	
	void setJdbcInitialPoolSize(int value);
	
	@Description("Path to Table-FHIR map config")
	@Default.String("../utils/dbz_event_to_fhir_config.json")
	String getFhirDebeziumConfigPath();
	
	void setFhirDebeziumConfigPath(String value);
	
	@Description("Flag to switch between the 2 modes of batch extract")
	@Default.Boolean(false)
	Boolean isJdbcModeEnabled();
	
	void setJdbcModeEnabled(Boolean value);
	
	@Description("The number of seconds after which records are flushed into Parquet/text files; "
	        + "use 0 to disable (note this may have undesired memory implications).")
	@Default.Integer(600)
	int getSecondsToFlushParquetFiles();
	
	void setSecondsToFlushParquetFiles(int value);
	
	@Description("The approximate size (bytes) of the row-groups in Parquet files. When this size is "
	        + "reached, the content is flushed to disk. This won't be triggered if there are less than 100 records.\n"
	        + "The default 0 uses the default row-group size of Parquet writers.")
	@Default.Integer(0)
	int getRowGroupSizeForParquetFiles();
	
	void setRowGroupSizeForParquetFiles(int value);
	
	@Description("The active period with format: 'DATE1_DATE2' OR 'DATE1'. The first form declares "
	        + "the first date-time (non-inclusive) and last date-time (inclusive); the second form declares "
	        + "the active period to be from the given date-time (non-inclusive) until now. Resources outside "
	        + "the active period are only fetched if they are associated with Patients in the active period. "
	        + "All requested resources in the active period are fetched.\n"
	        + "The date format follows the dateTime format in the FHIR standard, without the time-zone:\n"
	        + "https://www.hl7.org/fhir/datatypes.html#dateTime\n"
	        + "For example: --activePeriod=2020-11-10T00:00:00_2020-11-20\n"
	        + "Note this feature implies fetching Patient resources that were active in the given period.\n"
	        + "Default empty string disables this feature, i.e., all requested resources are fetched.")
	@Default.String("")
	String getActivePeriod();
	
	void setActivePeriod(String value);
}
