package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.common.R4UsCoreProfileData;
import com.cerner.bunsen.common.Stu3UsCoreProfileData;
import com.cerner.bunsen.exception.HapiMergeException;
import com.cerner.bunsen.exception.ProfileMapperException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AvroConverterMergeTest {

  @Before
  public void setUp() throws URISyntaxException, ProfileMapperException {
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.R4);
  }

  @Test
  public void validateMergedTestPatientSchema()
      throws ProfileMapperException, HapiMergeException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.R4, "/other-profile-definitions");

    List<String> patientProfiles =
        Arrays.asList(
            "http://hl7.org/fhir/StructureDefinition/Patient",
            "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient");

    AvroConverter mergedConverter = AvroConverter.forResources(fhirContext, patientProfiles);

    InputStream inputStream =
        this.getClass().getResourceAsStream("/other-schemas/bunsen-test-patient-schema.json");
    Schema expectedSchema = new Parser().parse(inputStream);

    Assert.assertEquals(expectedSchema.toString(), mergedConverter.getSchema().toString());
  }

  @Test
  public void validateMergedR4UsCoreSchemas()
      throws ProfileMapperException, HapiMergeException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.R4, "/r4-us-core-definitions");
    validateSchema(
        "/r4-us-core-schemas/us-core-patient-schema.json",
        R4UsCoreProfileData.US_CORE_PATIENT_PROFILES,
        fhirContext);
    validateSchema(
        "/r4-us-core-schemas/us-core-observation-schema.json",
        R4UsCoreProfileData.US_CORE_OBSERVATION_PROFILES,
        fhirContext);
    validateSchema(
        "/r4-us-core-schemas/us-core-condition-schema.json",
        R4UsCoreProfileData.US_CORE_CONDITION_PROFILES,
        fhirContext);
  }

  @Test
  public void validateMergedStu3UsCoreSchemas()
      throws ProfileMapperException, HapiMergeException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.DSTU3, "/stu3-us-core-definitions");
    validateSchema(
        "/stu3-us-core-schemas/us-core-patient-schema.json",
        Stu3UsCoreProfileData.US_CORE_PATIENT_PROFILES,
        fhirContext);
    validateSchema(
        "/stu3-us-core-schemas/us-core-observation-schema.json",
        Stu3UsCoreProfileData.US_CORE_OBSERVATION_PROFILES,
        fhirContext);
  }

  private void validateSchema(
      String expectedSchemaFile, List<String> profileResourceTypeUrls, FhirContext fhirContext)
      throws HapiMergeException, IOException {
    AvroConverter converter = AvroConverter.forResources(fhirContext, profileResourceTypeUrls);
    InputStream inputStream = this.getClass().getResourceAsStream(expectedSchemaFile);
    Schema expectedSchema = new Parser().parse(inputStream);
    Assert.assertEquals(expectedSchema.toString(), converter.getSchema().toString());
  }
}
