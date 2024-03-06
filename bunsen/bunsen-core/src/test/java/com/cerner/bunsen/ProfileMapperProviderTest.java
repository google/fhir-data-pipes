package com.cerner.bunsen;

import static org.hamcrest.MatcherAssert.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileMapperException;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;

public class ProfileMapperProviderTest {

  public static final String US_CORE_PATIENT_PROFILE =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  public static final String HEAD_OCCIPITAL_FRONT_CIRCUMFERENCE_PERCENTILE =
      "http://hl7.org/fhir/us/core/StructureDefinition/head-occipital-frontal-circumference-percentile";

  public static final String BASE_OBSERVATION_PROFILE =
      "http://hl7.org/fhir/StructureDefinition/Observation";

  public static final String BUNSEN_TEST_PATIENT_PROFILE =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient";

  @Test
  public void testIfR4ProfilesAreOverloaded() throws ProfileMapperException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);
    Map<String, String> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "/r4-us-core-definitions", true);

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(profileMapping.get("Patient"), Matchers.equalTo(US_CORE_PATIENT_PROFILE));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Observation"),
        Matchers.equalTo(HEAD_OCCIPITAL_FRONT_CIRCUMFERENCE_PERCENTILE));
  }

  @Test
  public void testIfStu3ProfilesAreOverloaded() throws ProfileMapperException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.DSTU3);
    Map<String, String> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "/other-profile-definitions", true);

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(profileMapping.get("Patient"), Matchers.equalTo(BUNSEN_TEST_PATIENT_PROFILE));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(profileMapping.get("Observation"), Matchers.equalTo(BASE_OBSERVATION_PROFILE));
  }
}
