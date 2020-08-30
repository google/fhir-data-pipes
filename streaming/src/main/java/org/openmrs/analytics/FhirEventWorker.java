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

import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import org.hl7.fhir.r4.model.BaseResource;
import org.hl7.fhir.r4.model.Resource;
import org.ict4h.atomfeed.client.domain.Event;
import org.ict4h.atomfeed.client.service.EventWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FhirEventWorker<T extends BaseResource> implements EventWorker {

	private static final Logger log = LoggerFactory.getLogger(FhirEventWorker.class);

	private String sourceUrl;

	private String resourceType;

	private Class<T> resourceClass;

	private FhirStoreUtil fhirStoreUtil;

	private String sourceUser;

	private String sourcePw;

	FhirEventWorker(String sourceUrl, Class<T> resourceClass, String sourceUser, String sourcePW, String targetUrl) {
		this.sourceUrl = sourceUrl;
		this.resourceClass = resourceClass;
		this.sourceUser = sourceUser;
		this.sourcePw = sourcePW;

		if(targetUrl.contains("projects"))
			this.fhirStoreUtil = new GcpStoreUtil(targetUrl, sourceUrl, sourceUser, sourcePW);
		else
			this.fhirStoreUtil = new HapiStoreUtil(targetUrl, sourceUrl, sourceUser, sourcePW);
	}

	private T fetchFhirResource(String resourceUrl) {
      try {
        // Create client
        IGenericClient client = fhirStoreUtil.getSourceClient();

        // Parse resourceUrl
        String[] sepUrl = resourceUrl.split("/");
        String resourceId = sepUrl[sepUrl.length-1];
        String resourceType = sepUrl[sepUrl.length-2];

        T resource = (T) client.read()
                .resource(resourceType)
                .withId(resourceId)
                .execute();

        return resource;
      } catch (Exception e) {
        System.out.println("Failed fetching FHIR resource at " + resourceUrl + ": " + e);
        return null;
      }
	}

	@Override
	public void process(Event event) {
      String content = event.getContent();
      if (content == null || "".equals(content)) {
        log.warn("No content in event: " + event);
        return;
      }
      try {
        JsonObject jsonObj = JsonParser.parseString(content).getAsJsonObject();
        String fhirUrl = jsonObj.get("fhir").getAsString();
        if (fhirUrl == null || "".equals(fhirUrl)) {
          log.info("Skipping non-FHIR event " + event);
          return;
        }

        if(!fhirUrl.contains("fhir2")) fhirUrl = fhirUrl.replace("/fhir", "/fhir2/R4");

        System.out.println("FHIR resource URL is: " + fhirUrl);
        T resource = fetchFhirResource(fhirUrl);
        String resourceId = resource.getIdElement().getResourceType() + "/" + resource.getIdElement().getIdPart();

        System.out.println(String.format("Parsed FHIR resource ID is %s", resourceId));

        fhirStoreUtil.uploadResourceToCloud(resourceId, (Resource) resource);
      } catch (JsonParseException e) {
        log.error(
                String.format("Error parsing event %s with error %s", event.toString(), e.toString()));
      }
	}

	@Override
	public void cleanUp(Event event) {
		log.info("In clean-up!");
	}

}
