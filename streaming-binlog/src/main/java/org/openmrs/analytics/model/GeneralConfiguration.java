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
package org.openmrs.analytics.model;

import java.util.ArrayList;
import java.util.List;

public class GeneralConfiguration {
	
	private List<EventConfiguration> eventConfigurations;
	
	public GeneralConfiguration() {
		eventConfigurations = new ArrayList<>();
	}
	
	public GeneralConfiguration(List<EventConfiguration> eventConfigurations) {
		this.eventConfigurations = eventConfigurations;
	}
	
	public List<EventConfiguration> getEventConfigurations() {
		return eventConfigurations;
	}
	
	public void setEventConfigurations(List<EventConfiguration> eventConfigurations) {
		this.eventConfigurations = eventConfigurations;
	}
}
