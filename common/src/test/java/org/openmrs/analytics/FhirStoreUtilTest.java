package org.openmrs.analytics;

import org.junit.Before;
import org.junit.Test;

public class FhirStoreUtilTest {

  @Before
  public void setup() {
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithMalformedStore() {
    FhirStoreUtil fhirStoreUtil = new GcpStoreUtil("test", "somesource", "someuser", "somepw");
  }

  @Test
  public void testConstructor() {
    FhirStoreUtil fhirStoreUtil = new GcpStoreUtil(
        "projects/my_project-123/locations/us-central1/datasets/openmrs_fhir_test/fhirStores/test", "somesource", "someuser", "somepw");
  }
}
