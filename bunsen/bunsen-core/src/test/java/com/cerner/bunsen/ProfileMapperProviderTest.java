package com.cerner.bunsen;

import static org.hamcrest.MatcherAssert.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.common.R4UsCoreProfileData;
import com.cerner.bunsen.common.Stu3UsCoreProfileData;
import com.cerner.bunsen.exception.ProfileException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;

public class ProfileMapperProviderTest {

  @Test
  public void testIfOnlyBaseProfilesAreMapped() throws ProfileException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);
    Map<String, List<String>> profileMapping =
        profileMappingProvider.loadStructureDefinitions(fhirContext, null);
    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    List<String> patientList = profileMapping.get("Patient");
    assertThat(
        patientList != null ? patientList.toArray() : null,
        Matchers.equalTo(
            Arrays.asList("http://hl7.org/fhir/StructureDefinition/Patient").toArray()));

    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    List<String> observationList = profileMapping.get("Observation");
    assertThat(
        observationList != null ? observationList.toArray() : null,
        Matchers.equalTo(
            Arrays.asList("http://hl7.org/fhir/StructureDefinition/Observation").toArray()));
  }

  @Test
  public void testIfUsCoreR4ProfilesAreMapped() throws ProfileException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);
    Map<String, List<String>> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "classpath:/r4-us-core-definitions");

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    List<String> patientList = profileMapping.get("Patient");
    assertThat(
        patientList != null ? patientList.toArray() : null,
        Matchers.arrayContainingInAnyOrder(R4UsCoreProfileData.US_CORE_PATIENT_PROFILES.toArray()));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    List<String> observationList = profileMapping.get("Observation");
    assertThat(
        observationList != null ? observationList.toArray() : null,
        Matchers.arrayContainingInAnyOrder(
            R4UsCoreProfileData.US_CORE_OBSERVATION_PROFILES.toArray()));
  }

  @Test
  public void testIfUsCoreStu3ProfilesAreMapped() throws ProfileException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.DSTU3);
    Map<String, List<String>> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "classpath:/stu3-us-core-definitions");

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Patient") != null ? profileMapping.get("Patient").toArray() : null,
        Matchers.arrayContainingInAnyOrder(R4UsCoreProfileData.US_CORE_PATIENT_PROFILES.toArray()));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    List<String> observationList = profileMapping.get("Observation");

    assertThat(
        observationList != null ? observationList.toArray() : null,
        Matchers.arrayContainingInAnyOrder(
            Stu3UsCoreProfileData.US_CORE_OBSERVATION_PROFILES.toArray()));
  }
}
