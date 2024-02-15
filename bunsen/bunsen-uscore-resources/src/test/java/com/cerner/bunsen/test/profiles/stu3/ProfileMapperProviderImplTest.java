package com.cerner.bunsen.test.profiles.stu3;

import static org.hamcrest.MatcherAssert.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.cerner.bunsen.profiles.ProfileMappingProvider;
import com.cerner.bunsen.profiles.ProfileMappingProviderImpl;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;

public class ProfileMapperProviderImplTest {

  public static final String US_CORE_PATIENT_PROFILE =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  public static final String BASE_OBSERVATION_PROFILE =
      "http://hl7.org/fhir/StructureDefinition/Observation";

  public static final String BUNSEN_TEST_PATIENT_PROFILE =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient";

  @Test
  public void testIfR4ProfilesAreOverloaded() throws URISyntaxException, ProfileMapperException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProviderImpl();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);
    URL resource =
        ProfileMapperProviderImplTest.class
            .getClassLoader()
            .getResource("definitions-r4/StructureDefinition-us-core-patient.json");

    File file = Paths.get(resource.toURI()).toFile();
    Map<String, String> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, file.getParentFile().getAbsolutePath());

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(profileMapping.get("Patient"), Matchers.equalTo(US_CORE_PATIENT_PROFILE));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(profileMapping.get("Observation"), Matchers.equalTo(BASE_OBSERVATION_PROFILE));
  }

  @Test
  public void testIfStu3ProfilesAreOverloaded() throws URISyntaxException, ProfileMapperException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProviderImpl();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.DSTU3);
    URL resource =
        ProfileMapperProviderImplTest.class
            .getClassLoader()
            .getResource("definitions/StructureDefinition-bunsen-test-profile-Patient.json");

    File file = Paths.get(resource.toURI()).toFile();
    Map<String, String> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, file.getParentFile().getAbsolutePath());

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(profileMapping.get("Patient"), Matchers.equalTo(BUNSEN_TEST_PATIENT_PROFILE));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(profileMapping.get("Observation"), Matchers.equalTo(BASE_OBSERVATION_PROFILE));
  }
}
