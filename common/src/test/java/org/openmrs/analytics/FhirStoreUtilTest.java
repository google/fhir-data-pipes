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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class FhirStoreUtilTest {
	
	@Mock
	FhirContext fhirContext;
	
	@Before
	public void setup() {
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithMalformedStore() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil("test", fhirContext);
	}
	
	@Test
	public void testConstructor() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(
		        "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test", fhirContext);
	}
	
	@Test
	public void testConstructorWithDefaultStore() {
		FhirStoreUtil fhirStoreUtil = new FhirStoreUtil("projects/P/locations/L/datasets/D/fhirStores/F", fhirContext);
	}
}
