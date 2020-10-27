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

import java.util.LinkedHashMap;

public class EventConfiguration {
	
	private boolean enabled;
	
	private String title;
	
	private String table;
	
	private LinkedHashMap<String, String> linkTemplates;
	
	public EventConfiguration() {
	}
	
	public EventConfiguration(String title, String table, LinkedHashMap<String, String> linkTemplates) {
		this.enabled = false;
		this.title = title;
		this.table = table;
		this.linkTemplates = linkTemplates;
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public String getTitle() {
		return title;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getTable() {
		return table;
	}
	
	public void setTable(String table) {
		this.table = table;
	}
	
	public LinkedHashMap<String, String> getLinkTemplates() {
		return linkTemplates;
	}
	
	public void setLinkTemplates(LinkedHashMap<String, String> linkTemplates) {
		this.linkTemplates = linkTemplates;
	}
}
