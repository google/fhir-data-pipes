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

public class BaseArgs {
	
	@Parameter(names = { "--openmrsUserName" }, description = "User name for openmrs server")
	public String openmrUserName = "admin";
	
	@Parameter(names = { "--openmrsPassword" }, description = "Password for openmrs User")
	public String openmrsPassword = "Admin123";
	
	@Parameter(names = { "--openmrsServerUrl" }, description = "Openmrs Server Base Url")
	public String openmrsServerUrl = "http://localhost:8099/openmrs";
	
	@Parameter(names = { "--fhirSinkPath" }, description = "Google cloud FHIR store or target generic fhir store")
	public String fhirSinkPath = "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME";
	
	@Parameter(names = { "--sinkUser" }, description = "Sink BasicAuth Username")
	public String sinkUser = "";
	
	@Parameter(names = { "--sinkPassword" }, description = "Sink BasicAuth Password")
	public String sinkPassword = "";
	
	@Parameter(names = { "--fileParquetPath" }, description = "The base path for output Parquet files")
	public String fileParquetPath;
	
	@Parameter(names = { "--secondsToFlushParquetFiles" }, description = "The number of seconds after which all Parquet "
	        + "writers with non-empty content are flushed to files; use 0 to disable.")
	public int secondsToFlushParquetFiles = 3600;
	
	@Parameter(names = { "--rowGroupSizeForParquetFiles" }, description = "The approximate size (bytes) of "
	        + "the row-groups in Parquet files. When this size is reached, the content is flushed to disk. "
	        + "This won't be triggered if there are less than 100 records. Use 0 to fall back to the "
	        + "default row-group size.")
	public int rowGroupSizeForParquetFiles = 0;
}
