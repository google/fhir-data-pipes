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
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import org.hl7.fhir.r4.model.Observation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FetchResourcesTest {
	
	private FetchResources fetchResources;
	
	private FhirContext fhirContext;
	
	private Observation observation;
	
	@Before
	public void setup() throws IOException {
		URL url = Resources.getResource("observation.json");
		String obsStr = Resources.toString(url, StandardCharsets.UTF_8);
		this.fhirContext = FhirContext.forR4();
		IParser parser = fhirContext.newJsonParser();
		observation = parser.parseResource(Observation.class, obsStr);
		// TODO BEFORE SUBMUT: remove it not needed!
		//  fetchResources = new FetchResources(null, "Observation", null);
	}
	
	@Test
	public void testGetPatientId() {
		String expectedId = "471be3bc-08c7-4d78-a4ab-1b3d044dae67";
		String patientId = FetchResources.getSubjectPatientIdOrNull(observation);
		assertThat(patientId, notNullValue());
		assertThat(patientId, equalTo(expectedId));
	}
	
}
