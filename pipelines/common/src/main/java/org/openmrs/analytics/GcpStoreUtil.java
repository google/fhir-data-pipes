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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends the functionality of FhirStoreUtil for communication with Google Cloud
 * Healthcare API services. The GCP communication requires the request to be authenticated and
 * contain specific headers. This class takes care of these idiosyncracies and then leverages the
 * generic communication patterns in FhirStoreUtil.
 *
 * @see org.openmrs.analytics.FhirStoreUtil
 */
class GcpStoreUtil extends FhirStoreUtil {
	
	private static final Logger log = LoggerFactory.getLogger(GcpStoreUtil.class);
	
	private static final GsonFactory JSON_FACTORY = new GsonFactory();
	
	private static final NetHttpTransport HTTP_TRANSPORT = new NetHttpTransport();
	
	protected GcpStoreUtil(String sinkUrl, IRestfulClientFactory clientFactory) {
		super(sinkUrl, "", "", clientFactory);
	}
	
	@Override
	public MethodOutcome uploadResource(Resource resource) {
		try {
			updateFhirResource(sinkUrl, resource);
		}
		catch (Exception e) {
			log.error(String.format("Exception while sending to sink: %s", e.toString()));
		}
		return null;
	}
	
	@Override
	public Collection<MethodOutcome> uploadBundle(Bundle bundle) {
		try {
			// Initialize the client, which will be used to interact with the service.
			CloudHealthcare client = createClient();
			String uri = String.format("%sv1/%s/fhir", client.getRootUrl(), sinkUrl);
			URIBuilder uriBuilder = new URIBuilder(uri);
			log.info("Full URL is: {}", uriBuilder.build());
			
			return super.uploadBundle(uri, bundle,
			    Collections.singletonList(new BearerTokenAuthInterceptor(getAccessToken())));
		}
		catch (IOException e) {
			log.error("IOException while using Google APIs: {}", e.toString(), e);
		}
		catch (URISyntaxException e) {
			log.error("URI syntax exception while using Google APIs: {}", e.toString(), e);
		}
		return null;
	}
	
	protected MethodOutcome updateFhirResource(String fhirStoreName, Resource resource) {
		try {
			// Initialize the client, which will be used to interact with the service.
			CloudHealthcare client = createClient();
			String uri = String.format("%sv1/%s/fhir", client.getRootUrl(), fhirStoreName);
			URIBuilder uriBuilder = new URIBuilder(uri);
			log.info(String.format("Full URL is: %s", uriBuilder.build()));
			
			return super.updateFhirResource(uri, resource,
			    Collections.<IClientInterceptor> singletonList(new BearerTokenAuthInterceptor(getAccessToken())));
		}
		catch (IOException e) {
			log.error(String.format("IOException while using Google APIs: %s", e.toString()));
		}
		catch (URISyntaxException e) {
			log.error(String.format("URI syntax exception while using Google APIs: %s", e.toString()));
		}
		return null;
	}
	
	private CloudHealthcare createClient() throws IOException {
		final GoogleCredential credential = getGoogleCredential();
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {
			
			@Override
			public void initialize(HttpRequest httpRequest) throws IOException {
				credential.initialize(httpRequest);
				httpRequest.setConnectTimeout(60000); // 1 minute connect timeout
				httpRequest.setReadTimeout(60000); // 1 minute read timeout
			}
		};
		
		// Build the client for interacting with the service.
		return new CloudHealthcare.Builder(HTTP_TRANSPORT, JSON_FACTORY, requestInitializer)
		        .setApplicationName("openmrs-fhir-warehouse").build();
	}
	
	private String getAccessToken() throws IOException {
		GoogleCredential credential = getGoogleCredential();
		credential.refreshToken();
		return credential.getAccessToken();
	}
	
	private GoogleCredential getGoogleCredential() throws IOException {
		/*
		// TODO figure out why scope creation fails in this case.
		// Use Application Default Credentials (ADC) to authenticate the requests
		// For more information see https://cloud.google.com/docs/authentication/production
		final GoogleCredentials credentials =
		GoogleCredentials.getApplicationDefault()//;
		    .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
		credentials.refreshAccessToken();
		return credentials.getAccessToken().getTokenValue();
		 */
		GoogleCredential credential = GoogleCredential.getApplicationDefault(HTTP_TRANSPORT, JSON_FACTORY)
		        .createScoped(Collections.singleton(CloudHealthcareScopes.CLOUD_PLATFORM));
		return credential;
	}
	
}
