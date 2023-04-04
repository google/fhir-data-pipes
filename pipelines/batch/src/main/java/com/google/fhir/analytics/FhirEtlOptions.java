/*
 * Copyright 2020-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Options supported by {@link FhirEtl}. */
public interface FhirEtlOptions extends PipelineOptions {

  @Description("Fhir source server URL, e.g., http://localhost:8091/fhir, etc.")
  @Required
  @Default.String("")
  String getFhirServerUrl();

  void setFhirServerUrl(String value);

  @Description("Comma separated list of resource to fetch, e.g., 'Patient,Encounter,Observation'.")
  @Default.String("Patient,Encounter,Observation")
  String getResourceList();

  void setResourceList(String value);

  @Description(
      "The number of resources to be fetched in one API call. "
          + "For the JDBC mode passing > 170 could result in HTTP 400 Bad Request. "
          + "Note by default the maximum bundle size for OpenMRS FHIR module is 100.")
  @Default.Integer(100)
  int getBatchSize();

  void setBatchSize(int value);

  @Description(
      "This flag is used in the JDBC mode. In the context of an OpenMRS source, this is the size"
          + " of each ID chunk. In the context of a HAPI source, this is the size of each database"
          + " query. Setting high values (~10000 for OpenMRS, ~1000 for HAPI) will yield faster"
          + " query execution.")
  @Default.Integer(1000)
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

  @Description("OAuth OIDC Connect URL.")
  @Required
  @Default.String("")
  String getOidConnectUrl();

  void setOidConnectUrl(String value);

  @Description("Oauth Client ID.")
  @Default.String("")
  String getClientId();

  void setClientId(String value);

  @Description("OAuth Client Secret.")
  @Default.String("")
  String getClientSecret();

  void setClientSecret(String value);

  @Description("OAuth Password Credentials Username.")
  @Default.String("")
  String getOAuthUsername();

  void setOAuthUsername(String value);

  @Description("OAuth Password Credentials Password.")
  @Default.String("")
  String getOAuthPassword();

  void setOAuthPassword(String value);

  @Description(
      "The path to the target generic fhir store, or a GCP fhir store with the format:"
          + " `projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+`, e.g., "
          + "`projects/my-project/locations/us-central1/datasets/fhir_test/fhirStores/test`")
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

  @Description(
      "The base name for output Parquet files; for each resource, one fileset will be created.")
  @Default.String("")
  String getOutputParquetPath();

  void setOutputParquetPath(String value);

  @Description("JDBC maximum pool size")
  @Default.Integer(50)
  int getJdbcMaxPoolSize();

  void setJdbcMaxPoolSize(int value);

  @Description("JDBC initial pool size")
  @Default.Integer(10)
  int getJdbcInitialPoolSize();

  void setJdbcInitialPoolSize(int value);

  @Description(
      "Path to FHIR database config for JDBC mode; the default value file (i.e.,"
          + " hapi-postgres-config.json) is for a HAPI server with PostgreSQL database. There is"
          + " also a sample file for an OpenMRS server with MySQL database"
          + " (dbz_event_to_fhir_config.json); the Debezium config can be ignored for batch.")
  @Default.String("../utils/hapi-postgres-config.json")
  String getFhirDatabaseConfigPath();

  void setFhirDatabaseConfigPath(String value);

  @Description("Flag to switch between the 2 modes of batch extract")
  @Default.Boolean(false)
  Boolean isJdbcModeEnabled();

  void setJdbcModeEnabled(Boolean value);

  @Description(
      "Flag to use jdbc mode batch extract for a HAPI source; this implies --jdbcModeEnabled")
  @Default.Boolean(false)
  Boolean isJdbcModeHapi();

  void setJdbcModeHapi(Boolean value);

  @Description(
      "The number of seconds after which records are flushed into Parquet/text files; "
          + "use 0 to disable (note this may have undesired memory implications).")
  @Default.Integer(600)
  int getSecondsToFlushParquetFiles();

  void setSecondsToFlushParquetFiles(int value);

  @Description(
      "The approximate size (bytes) of the row-groups in Parquet files. When this size is reached,"
          + " the content is flushed to disk. This won't be triggered if there are less than 100"
          + " records.\n"
          + "The default 0 uses the default row-group size of Parquet writers.")
  @Default.Integer(0)
  int getRowGroupSizeForParquetFiles();

  void setRowGroupSizeForParquetFiles(int value);

  // TODO: Either remove this feature or properly implement patient history fetching based on
  //   Patient Compartment definition.
  @Description(
      "The active period with format: 'DATE1_DATE2' OR 'DATE1'. The first form declares the first"
          + " date-time (non-inclusive) and last date-time (inclusive); the second form declares"
          + " the active period to be from the given date-time (non-inclusive) until now."
          + " Resources outside the active period are only fetched if they are associated with"
          + " Patients in the active period. All requested resources in the active period are"
          + " fetched.\n"
          + "The date format follows the dateTime format in the FHIR standard, without time-zone:\n"
          + "https://www.hl7.org/fhir/datatypes.html#dateTime\n"
          + "For example: --activePeriod=2020-11-10T00:00:00_2020-11-20\n"
          + "Note this feature implies fetching Patient resources that were active in the given"
          + " period.\n"
          + "Default empty string disables this feature, i.e., all requested resources are"
          + " fetched.")
  @Default.String("")
  String getActivePeriod();

  void setActivePeriod(String value);

  @Description(
      "Fetch only FHIR resources that were updated after the given timestamp."
          + "The date format follows the dateTime format in the FHIR standard, without time-zone:\n"
          + "https://www.hl7.org/fhir/datatypes.html#dateTime\n"
          + "This feature is currently implemented only for HAPI JDBC mode.")
  @Default.String("")
  String getSince();

  void setSince(String value);

  // NOTE: Sink DB options are experimental.
  @Description("Path to the sink database config; if not set, no sink DB is used [experimental].")
  @Default.String("")
  String getSinkDbConfigPath();

  void setSinkDbConfigPath(String value);

  @Description("The name prefix for the sink DB tables.")
  @Default.String("")
  String getSinkDbTablePrefix();

  void setSinkDbTablePrefix(String value);

  @Description(
      "If enabled all json resources are stored in the same table; by default a separate "
          + "table is created for each resource type.")
  @Default.Boolean(false)
  Boolean getUseSingleSinkTable();

  void setUseSingleSinkTable(Boolean value);

  @Description(
      "The pattern for input JSON files, e.g., 'PATH/*'. Each file should be one Bundle resource.")
  @Default.String("")
  String getSourceJsonFilePattern();

  void setSourceJsonFilePattern(String value);
}
