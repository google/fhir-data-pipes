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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.interceptor.AdditionalRequestHeadersInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirStoreUtil {
	
	private static final Logger log = LoggerFactory.getLogger(FhirStoreUtil.class);
	
	protected IRestfulClientFactory clientFactory;
	
	protected final String sinkUrl;
	
	private final String sinkUsername;
	
	private final String sinkPassword;
	
	private static final Pattern GCP_PATTERN = Pattern
	        .compile("projects/[\\w-]+/locations/[\\w-]+/datasets/[\\w-]+/fhirStores/[\\w-]+");
	
	protected FhirStoreUtil(String sinkUrl, String sinkUsername, String sinkPassword, IRestfulClientFactory clientFactory) {
		this.clientFactory = clientFactory;
		this.sinkUrl = sinkUrl;
		this.sinkUsername = sinkUsername;
		this.sinkPassword = sinkPassword;
	}
	
	public static FhirStoreUtil createFhirStoreUtil(String sinkUrl, IRestfulClientFactory clientFactory)
	        throws IllegalArgumentException {
		return createFhirStoreUtil(sinkUrl, "", "", clientFactory);
	}
	
	public static FhirStoreUtil createFhirStoreUtil(String sinkUrl, String sinkUsername, String sinkPassword,
	        IRestfulClientFactory clientFactory) throws IllegalArgumentException {
		if (matchesGcpPattern(sinkUrl)) {
			return new GcpStoreUtil(sinkUrl, sinkUsername, sinkPassword, clientFactory);
		} else {
			return new FhirStoreUtil(sinkUrl, sinkUsername, sinkPassword, clientFactory);
		}
	}
	
	public MethodOutcome uploadResource(Resource resource) {
		Collection<IClientInterceptor> interceptors = Collections.<IClientInterceptor> emptyList();
		
		if (sinkUsername.isEmpty() && !sinkPassword.isEmpty()) {
			interceptors = Collections.<IClientInterceptor> singleton(new BasicAuthInterceptor(sinkUsername, sinkPassword));
		}
		
		try {
			return updateFhirResource(sinkUrl, resource, interceptors);
		}
		catch (Exception e) {
			log.error(String.format("Exception while sending to sink: %s", e.toString()));
			return null;
		}
	}
	
	public Collection<MethodOutcome> uploadBundle(Bundle bundle) {
		List<MethodOutcome> responses = new ArrayList<MethodOutcome>(bundle.getTotal());
		
		for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
			Resource resource = entry.getResource();
			responses.add(uploadResource(resource));
		}
		return responses;
	}
	
	protected MethodOutcome updateFhirResource(String targetUri, Resource resource,
	        Collection<IClientInterceptor> interceptors) {
		
		IGenericClient client = clientFactory.newGenericClient(targetUri);
		
		for (IClientInterceptor interceptor : interceptors) {
			client.registerInterceptor(interceptor);
		}
		
		AdditionalRequestHeadersInterceptor interceptor = new AdditionalRequestHeadersInterceptor();
		interceptor.addHeaderValue("Accept", "application/fhir+json");
		interceptor.addHeaderValue("Accept-Charset", "utf-8");
		interceptor.addHeaderValue("Content-Type", "application/fhir+json");
		
		client.registerInterceptor(interceptor);
		
		// Initialize the client, which will be used to interact with the service.
		MethodOutcome outcome = client.create().resource(resource).encodedJson().execute();
		
		log.debug("FHIR resource created at" + targetUri + "? " + outcome.getCreated());
		
		return outcome;
	}
	
	static boolean matchesGcpPattern(String gcpFhirStore) {
		return GCP_PATTERN.matcher(gcpFhirStore).matches();
	}
}
