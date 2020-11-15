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
	public String openmrsPassword = "admin";
	
	@Parameter(names = { "--openmrsServerUrl" }, description = "Openmrs Server Base Url")
	public String openmrsServerUrl = "http://localhost:8099";
	
	@Parameter(names = { "--fhirSinkPath" }, description = "Google cloud FHIR store")
	public String fhirSinkPath = "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME";
	
	@Parameter(names = { "--sinkUser" }, description = "Sink BasicAuth Username")
	public String sinkUser = "hapi";
	
	@Parameter(names = { "--sinkPassword" }, description = "Sink BasicAuth Password")
	public String sinkPassword = "hapi";
	
	@Parameter(names = { "--fileParquetPath" }, description = "Google cloud FHIRE store")
	public String fileParquetPath = "/tmp/";
}
