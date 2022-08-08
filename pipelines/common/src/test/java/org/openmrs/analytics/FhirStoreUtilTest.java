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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.gclient.IUpdateTyped;
import org.hamcrest.Matchers;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirStoreUtilTest {
	
	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private IRestfulClientFactory clientFactory;
	
	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private IGenericClient client;
	
	@Mock
	IUpdateTyped iexec;
	
	private FhirStoreUtil fhirStoreUtil;
	
	private Patient patient;
	
	private Bundle patientBundle;
	
	private Bundle patientResponseBundle;
	
	@Before
	public void setup() throws IOException {
		IParser jsonParser = FhirContext.forR4().newJsonParser();
		
		try (InputStream patientBundleStream = getClass().getClassLoader().getResourceAsStream("patient_bundle.json")) {
			patientBundle = jsonParser.parseResource(Bundle.class, patientBundleStream);
		}
		
		try (InputStream patientResponseBundleStream = getClass().getClassLoader()
		        .getResourceAsStream("patient_response_bundle.json")) {
			patientResponseBundle = jsonParser.parseResource(Bundle.class, patientResponseBundleStream);
		}
		
		MethodOutcome outcome = new MethodOutcome();
		outcome.setCreated(true);
		String sinkUrl = "test";
		
		patient = (Patient) patientBundle.getEntryFirstRep().getResource();
		
		when(clientFactory.newGenericClient(sinkUrl)).thenReturn(client);
		when(client.update().resource(patient).withId(patient.getId()).encodedJson()).thenReturn(iexec);
		when(client.transaction().withBundle(ArgumentMatchers.any(Bundle.class)).execute())
		        .thenReturn(patientResponseBundle);
		doReturn(outcome).when(iexec).execute();
		
		fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(sinkUrl, clientFactory);
	}
	
	@Test
	public void testFactoryForFhirStore() {
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil("test", clientFactory);
		
		assertThat(fhirStoreUtil, Matchers.<FhirStoreUtil> instanceOf(FhirStoreUtil.class));
	}
	
	@Test
	public void testFactoryForGcpStore() {
		FhirStoreUtil fhirStoreUtil = FhirStoreUtil.createFhirStoreUtil(
		    "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test", clientFactory);
		
		assertThat(fhirStoreUtil, Matchers.<FhirStoreUtil> instanceOf(GcpStoreUtil.class));
	}
	
	@Test
	public void testUploadResource() {
		MethodOutcome result = fhirStoreUtil.uploadResource(patient);
		
		assertThat(result.getCreated(), equalTo(true));
	}
	
	@Test
	public void testUploadBundle() {
		Collection<MethodOutcome> result = fhirStoreUtil.uploadBundle(patientBundle);
		
		assertThat(result, not(nullValue()));
		assertThat(result, not(Matchers.empty()));
		assertThat(result.iterator().next().getCreated(), equalTo(true));
	}
	
}
