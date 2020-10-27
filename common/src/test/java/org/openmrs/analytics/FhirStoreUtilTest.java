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
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

import java.io.IOException;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.gclient.ICreateTyped;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Patient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirStoreUtilTest {
	
	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private IRestfulClientFactory clientFactory;
	
	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private IGenericClient client;
	
	@Mock
	ICreateTyped iexec;
	
	private FhirStoreUtil fhirStoreUtil;
	
	private Patient patient;
	
	@Before
	public void setup() throws IOException {
		FhirContext parseContext = FhirContext.forDstu3();
		Bundle patientBundle = parseContext.newJsonParser().parseResource(Bundle.class,
		    getClass().getClassLoader().getResource("patient_bundle.json").openStream());
		MethodOutcome outcome = new MethodOutcome();
		outcome.setCreated(true);
		String sinkUrl = "test";
		
		patient = (Patient) patientBundle.getEntryFirstRep().getResource();
		
		when(clientFactory.newGenericClient(sinkUrl)).thenReturn(client);
		when(client.create().resource(patient).encodedJson()).thenReturn(iexec);
		doReturn(outcome).when(iexec).execute();
		
		fhirStoreUtil = new FhirStoreUtil(sinkUrl, clientFactory);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithMalformedStore() {
		FhirStoreUtil fhirStoreUtil = new GcpStoreUtil("test", clientFactory);
	}
	
	@Test
	public void testConstructor() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(
		        "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test", clientFactory);
	}
	
	@Test
	public void testConstructorWithDefaultStore() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil("projects/P/locations/L/datasets/D/fhirStores/F", clientFactory);
	}
	
	@Test
	public void testUploadResourceToCloud() {
		MethodOutcome result = fhirStoreUtil.uploadResourceToCloud(patient);
		
		assertThat(result.getCreated(), equalTo(true));
	}
}
