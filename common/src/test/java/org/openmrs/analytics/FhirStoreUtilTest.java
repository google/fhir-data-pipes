package org.openmrs.analytics;

import org.junit.Before;
import org.junit.Test;

public class FhirStoreUtilTest {

  @Before
  public void setup() {
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithMalformedStore() {
    FhirStoreUtil fhirStoreUtil = new FhirStoreUtil("test");
  }

  @Test
  public void testConstructor() {
    FhirStoreUtil fhirStoreUtil = new FhirStoreUtil(
        "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test");
  }
}
