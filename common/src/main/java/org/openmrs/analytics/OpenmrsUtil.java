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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for getting fhir resources from Openmrs.
 */
public class OpenmrsUtil {
	
	private static final Logger log = LoggerFactory.getLogger(OpenmrsUtil.class);
	
	private final String fhirUrl;
	
	private final String sourceUser;
	
	private final String sourcePw;
	
	private final FhirContext fhirContext;
	
	OpenmrsUtil(String sourceFhirUrl, String sourceUser, String sourcePw, FhirContext fhirContext)
	        throws IllegalArgumentException {
		this.fhirUrl = sourceFhirUrl;
		this.sourceUser = sourceUser;
		this.sourcePw = sourcePw;
		this.fhirContext = fhirContext;
	}
	/**
	 * Fetches the fhir resource from openmrs courtesy of the openmrs FHIR2 module.
	 */
	public Resource fetchFhirResource(String resourceUrl) {
		try {
			// Create client
			IGenericClient client = getSourceClient();
			
			// Parse resourceUrl
			String[] sepUrl = resourceUrl.split("/");
			String resourceId = sepUrl[sepUrl.length - 1];
			String resourceType = sepUrl[sepUrl.length - 2];
			
			// TODO add summary mode for data only.
			IBaseResource resource = client.read().resource(resourceType).withId(resourceId).execute();
			
			return (Resource) resource;
		}
		catch (Exception e) {
			log.error("Failed fetching FHIR resource at " + resourceUrl + ": " + e);
			return null;
		}
	}
	
	public IGenericClient getSourceClient() {
		IClientInterceptor authInterceptor = new BasicAuthInterceptor(this.sourceUser, this.sourcePw);
		fhirContext.getRestfulClientFactory().setSocketTimeout(200 * 1000);
		
		IGenericClient client = fhirContext.getRestfulClientFactory().newGenericClient(this.fhirUrl);
		client.registerInterceptor(authInterceptor);
		
		return client;
	}
	
	public String getSourceFhirUrl() {
		return fhirUrl;
	}
	
}
