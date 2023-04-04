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

import com.beust.jcommander.Parameter;

public class BaseArgs {

  @Parameter(
      names = {"--fhirServerUserName"},
      description = "User name for fhir source server")
  public String fhirServerUserName = "admin";

  @Parameter(
      names = {"--fhirServerPassword"},
      description = "Password for openmrs User")
  public String fhirServerPassword = "Admin123";

  @Parameter(
      names = {"--fhirServerUrl"},
      description = "Fhir source server base Url")
  public String fhirServerUrl = "http://localhost:8099/openmrs/ws/fhir2/R4";

  @Parameter(
      names = {"--oidConnectUrl"},
      description = "OAuth OIDC Connect URL.")
  public String oidConnectUrl = "";

  @Parameter(
      names = {"--clientId"},
      description = "Oauth Client ID.")
  public String clientId = "";

  @Parameter(
      names = {"--clientSecret"},
      description = "OAuth Client Secret.")
  public String clientSecret = "";

  @Parameter(
      names = {"--oAuthUsername"},
      description = "OAuth Password Credentials Username.")
  public String oAuthUsername = "";

  @Parameter(
      names = {"--oAuthPassword"},
      description = "OAuth Password Credentials Password.")
  public String oAuthPassword = "";

  @Parameter(
      names = {"--fhirSinkPath"},
      description = "Google cloud FHIR store or target generic fhir store")
  public String fhirSinkPath = "";

  @Parameter(
      names = {"--sinkUserName"},
      description = "Sink BasicAuth Username")
  public String sinkUserName = "";

  @Parameter(
      names = {"--sinkPassword"},
      description = "Sink BasicAuth Password")
  public String sinkPassword = "";

  @Parameter(
      names = {"--outputParquetPath"},
      description = "The base path for output Parquet files")
  public String outputParquetPath = "";

  @Parameter(
      names = {"--secondsToFlushParquetFiles"},
      description =
          "The number of seconds after which all Parquet "
              + "writers with non-empty content are flushed to files; use 0 to disable.")
  public int secondsToFlushParquetFiles = 3600;

  @Parameter(
      names = {"--rowGroupSizeForParquetFiles"},
      description =
          "The approximate size (bytes) of the row-groups in Parquet files. When this size is"
              + " reached, the content is flushed to disk. This won't be triggered if there are"
              + " less than 100 records. Use 0 to fall back to the default row-group size.")
  public int rowGroupSizeForParquetFiles = 0;
}
