package com.cerner.bunsen.test.profiles.stu3;

import static org.hamcrest.MatcherAssert.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FileProfileProvider;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.Test;

public class FileProfileProviderTest {

  public static final String US_CORE_PATIENT_PROFILE =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  public static final String BASE_OBSERVATION_PROFILE =
      "http://hl7.org/fhir/StructureDefinition/Observation";

  @Test
  public void testIfProfilesAreOverloaded() throws URISyntaxException {
    FileProfileProvider profileProvider = new FileProfileProvider();
    FhirContext fhirContext = new FhirContext(FhirVersionEnum.R4);

    URL resource =
        FileProfileProviderTest.class
            .getClassLoader()
            .getResource("definitions-r4/StructureDefinition-us-core-patient.json");
    File file = Paths.get(resource.toURI()).toFile();
    Map<String, String> profileMapping =
        profileProvider.loadStructureDefinitions(
            fhirContext, Arrays.asList(file.getParentFile().getAbsolutePath()));

    assertThat(profileMapping.get("Patient"), Matchers.notNullValue());
    assertThat(profileMapping.get("Patient"), Matchers.equalTo(US_CORE_PATIENT_PROFILE));

    // Observation profile is not overloaded since no custom profile was defined
    assertThat(profileMapping.get("Observation"), Matchers.notNullValue());
    assertThat(profileMapping.get("Observation"), Matchers.equalTo(BASE_OBSERVATION_PROFILE));
  }
}
