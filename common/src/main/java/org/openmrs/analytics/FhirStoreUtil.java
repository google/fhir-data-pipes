package org.openmrs.analytics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirStoreUtil {
	protected static final Logger log = LoggerFactory.getLogger(HapiStoreUtil.class);
	protected static FhirContext fhirContext = FhirContext.forR4();

	protected String targetFhirStoreUrl;
	protected String sourceFhirUrl;
	protected String sourceUser;
	protected String sourcePw;

	public FhirStoreUtil(String targetFhirStoreUrl, String sourceFhirUrl, String sourceUser, String sourcePw) {
		this.targetFhirStoreUrl = targetFhirStoreUrl;
		this.sourceFhirUrl = sourceFhirUrl;
		this.sourceUser = sourceUser;
		this.sourcePw = sourcePw;
	}

	// This follows the examples at:
	// https://github.com/GoogleCloudPlatform/java-docs-samples/healthcare/tree/master/healthcare/v1
	public void uploadResourceToCloud(String resourceId, Resource resource) {
	  try {
	    updateFhirResource(this.targetFhirStoreUrl, resourceId, resource);
	  } catch (Exception e) {
	    System.out.println(
	            String.format("Exception while sending to sink: %s", e.toString()));
	  }
	}

	private void updateFhirResource(String fhirStoreName,
			String resourceId, Resource resource) throws IOException, URISyntaxException {
		IGenericClient client = fhirContext.newRestfulGenericClient(this.targetFhirStoreUrl);

		resource.setId(resourceId);

		// Initialize the client, which will be used to interact with the service.
  		MethodOutcome outcome = client.update()
				.resource(resource)
				.withId(resourceId)
				.prettyPrint()
				.encodedJson()
				.execute();

		System.out.println("Update FHIR resource create at" + this.targetFhirStoreUrl + "? "  + outcome.getCreated());
	}

	public FhirContext getFhirContext() {
	  return fhirContext;
	}

	public IGenericClient getSourceClient() {
		IClientInterceptor authInterceptor = new BasicAuthInterceptor(this.sourceUser, this.sourcePw);
		fhirContext.getRestfulClientFactory().setSocketTimeout(200 * 1000);

		IGenericClient client = fhirContext.newRestfulGenericClient(this.sourceFhirUrl);
		client.registerInterceptor(authInterceptor);

		return client;
	}

	public String executeRequest(HttpUriRequest request) {
		try {
			// Execute the request and process the results.
			HttpClient httpClient = HttpClients.createDefault();
			HttpResponse response = httpClient.execute(request);
			HttpEntity responseEntity = response.getEntity();
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			responseEntity.writeTo(byteStream);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_CREATED
					&& response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				log.error(String.format(
						"Exception for resource %s: %s", request.getURI().toString(),
						response.getStatusLine().toString()));
				log.error(byteStream.toString());
				throw new RuntimeException();
			}
			return byteStream.toString();
		} catch (IOException e) {
			log.error("Error in opening url: " + request.getURI().toString() + " exception: " + e);
			return "";
		}
	}

}
