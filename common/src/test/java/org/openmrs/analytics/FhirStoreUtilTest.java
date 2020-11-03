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
import ca.uhn.fhir.model.api.IResource;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IClientExecutable;
import ca.uhn.fhir.rest.gclient.IUpdateExecutable;
import com.google.common.io.Resources;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Resource;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class FhirStoreUtilTest {
	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private IGenericClient client;

	private FhirStoreUtil fhirStoreUtil;

	private Patient patient;

	@Before
	public void setup() throws IOException {
		FhirContext parseContext = FhirContext.forDstu3();
		Bundle patientBundle = parseContext.newJsonParser()
				.parseResource(Bundle.class, getClass().getClassLoader().getResource("patient_bundle.json").openStream());
		MethodOutcome outcome = new MethodOutcome();
		outcome.setCreated(true);

		patient = (Patient) patientBundle.getEntryFirstRep().getResource();

		when(client.update().resource(patient).withId(patient.getIdElement().getIdPart()).execute()).thenReturn(outcome);

		fhirStoreUtil = new FhirStoreUtil("", client);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithMalformedStore() {
		FhirStoreUtil fhirStoreUtil = new GcpStoreUtil("test", client);
	}
	
	@Test
	public void testConstructor() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(
		        "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test", client);
	}
	
	@Test
	public void testConstructorWithDefaultStore() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil("projects/P/locations/L/datasets/D/fhirStores/F", client);
	}

	@Test
	public void testUploadResourceToCloud() {
		MethodOutcome result = fhirStoreUtil.uploadResourceToCloud(patient);

		assertThat(result.getCreated(), equalTo(true));
	}
}
