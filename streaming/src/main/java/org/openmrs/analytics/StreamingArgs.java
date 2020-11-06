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
public class StreamingArgs {
	
	@Parameter(names = { "--openmrUserName" }, description = "user name for openmrs server")
	public String openmrUserName = "admin";
	
	@Parameter(names = { "--openmrsPassword" }, description = "Password for openmrs User")
	public String openmrsPassword = "admin";
	
	@Parameter(names = { "--openmrsServerUrl" }, description = "openmrs Server Base Url")
	public String openmrsServerUrl = "http://localhost:8099";
	
	@Parameter(names = { "--cloudGcpFhirStore" }, description = "Google cloud FHIRE store")
	public String cloudGcpFhirStore = "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME";
}
