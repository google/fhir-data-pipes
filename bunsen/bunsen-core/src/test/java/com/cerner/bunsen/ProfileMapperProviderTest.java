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
        profileMappingProvider.loadStructureDefinitions(fhirContext, null, false);
    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Patient").toArray(),
        Matchers.equalTo(
            Arrays.asList("http://hl7.org/fhir/StructureDefinition/Patient").toArray()));

    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Observation").toArray(),
        Matchers.equalTo(
            Arrays.asList("http://hl7.org/fhir/StructureDefinition/Observation").toArray()));
  }

  @Test
  public void testIfUsCoreR4ProfilesAreMapped() throws ProfileException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);
    Map<String, List<String>> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "/r4-us-core-definitions", true);

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Patient").toArray(),
        Matchers.arrayContainingInAnyOrder(R4UsCoreProfileData.US_CORE_PATIENT_PROFILES.toArray()));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Observation").toArray(),
        Matchers.arrayContainingInAnyOrder(
            R4UsCoreProfileData.US_CORE_OBSERVATION_PROFILES.toArray()));
  }

  @Test
  public void testIfUsCoreStu3ProfilesAreMapped() throws ProfileException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.DSTU3);
    Map<String, List<String>> profileMapping =
        profileMappingProvider.loadStructureDefinitions(
            fhirContext, "/stu3-us-core-definitions", true);

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(
        profileMapping.get("Patient").toArray(),
        Matchers.arrayContainingInAnyOrder(R4UsCoreProfileData.US_CORE_PATIENT_PROFILES.toArray()));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    ;
    assertThat(
        profileMapping.get("Observation").toArray(),
        Matchers.arrayContainingInAnyOrder(
            Stu3UsCoreProfileData.US_CORE_OBSERVATION_PROFILES.toArray()));
  }
}
