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

import java.util.Collection;
import java.util.Collections;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IUpdateExecutable;
import org.hl7.fhir.dstu3.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirStoreUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FhirStoreUtil.class);
	
	protected IGenericClient client;
	
	protected String sinkUrl;
	
	public FhirStoreUtil(String sinkUrl, IGenericClient client) throws IllegalArgumentException {
		this.client = client;
		this.sinkUrl = sinkUrl;
	}
	
	public MethodOutcome uploadResourceToCloud(Resource resource) {
		try {
			return updateFhirResource(sinkUrl, resource, Collections.<IClientInterceptor> emptyList());
		}
		catch (Exception e) {
			System.out.println(String.format("Exception while sending to sink: %s", e.toString()));
			return null;
		}
	}
	
	protected MethodOutcome updateFhirResource(String sinkUrl, Resource resource, Collection<IClientInterceptor> interceptors) {
		for (IClientInterceptor interceptor : interceptors) {
			client.registerInterceptor(interceptor);
		}
		
		// Initialize the client, which will be used to interact with the service.
		MethodOutcome outcome = client.update().resource(resource).withId(resource.getIdElement().getIdPart()).execute();

		log.debug("FHIR resource created at" + sinkUrl + "? " + outcome.getCreated());

		return outcome;
	}
}
