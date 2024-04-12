package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.common.R4UsCoreProfileData;
import com.cerner.bunsen.common.Stu3UsCoreProfileData;
import com.cerner.bunsen.exception.ProfileException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.IndexedRecord;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Patient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AvroConverterMergeTest {

  @Before
  public void setUp() throws URISyntaxException, ProfileException {
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.R4);
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.DSTU3);
  }

  @Test
  public void validateMergedR4CustomPatientSchema() throws ProfileException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.R4, "/r4-custom-profile-definitions");

    List<String> patientProfiles =
        Arrays.asList(
            "http://hl7.org/fhir/StructureDefinition/Patient",
            "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient");

    AvroConverter mergedConverter = AvroConverter.forResources(fhirContext, patientProfiles);

    InputStream inputStream =
        this.getClass().getResourceAsStream("/r4-custom-schemas/bunsen-test-patient-schema.json");
    Schema expectedSchema = new Parser().parse(inputStream);

    Assert.assertEquals(expectedSchema, mergedConverter.getSchema());
  }

  @Test
  public void validateMergedStu3CustomPatientSchema() throws ProfileException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.DSTU3, "/stu3-custom-profile-definitions");

    List<String> patientProfiles =
        Arrays.asList(
            "http://hl7.org/fhir/StructureDefinition/Patient",
            "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient");

    AvroConverter mergedConverter = AvroConverter.forResources(fhirContext, patientProfiles);

    InputStream inputStream =
        this.getClass().getResourceAsStream("/stu3-custom-schemas/bunsen-test-patient-schema.json");
    Schema expectedSchema = new Parser().parse(inputStream);

    Assert.assertEquals(expectedSchema, mergedConverter.getSchema());
  }

  @Test
  public void validateMergedR4UsCoreSchemas() throws ProfileException, IOException {
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
  public void validateMergedStu3UsCoreSchemas() throws ProfileException, IOException {
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

  @Test
  public void validateR4UsCoreResourceWithExtension() throws ProfileException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.R4, "/r4-us-core-definitions");

    AvroConverter patientConverter =
        AvroConverter.forResources(fhirContext, R4UsCoreProfileData.US_CORE_PATIENT_PROFILES);
    Patient patient =
        (Patient)
            loadResource(fhirContext, "/r4-us-core-resources/patient_us_core.json", Patient.class);
    IndexedRecord avroRecord = patientConverter.resourceToAvro(patient);
    Patient patientDecoded = (Patient) patientConverter.avroToResource(avroRecord);
    // TODO: When the objects are converted from HAPI->Avro->HAPI, in the resource ID only the
    //  IDElement part is retained. This needs to be consistent and is tracked here:
    // https://github.com/google/fhir-data-pipes/issues/1003
    patientDecoded.setId(patient.getIdElement());
    Assert.assertTrue(patient.equalsDeep(patientDecoded));
  }

  @Test
  public void validateMergedStu3UsCoreResourceWithExtensions()
      throws ProfileException, IOException {
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFromClasspathFor(FhirVersionEnum.DSTU3, "/stu3-us-core-definitions");

    AvroConverter patientConverter =
        AvroConverter.forResources(fhirContext, Stu3UsCoreProfileData.US_CORE_PATIENT_PROFILES);
    org.hl7.fhir.dstu3.model.Patient patient =
        (org.hl7.fhir.dstu3.model.Patient)
            loadResource(
                fhirContext,
                "/stu3-us-core-resources/patient_us_core.json",
                org.hl7.fhir.dstu3.model.Patient.class);
    IndexedRecord avroRecord = patientConverter.resourceToAvro(patient);
    org.hl7.fhir.dstu3.model.Patient patientDecoded =
        (org.hl7.fhir.dstu3.model.Patient) patientConverter.avroToResource(avroRecord);
    patientDecoded.setId(patient.getId());
    // TODO : The text field is not properly copied to the decoded object back, hence manually
    // copying it, check here for details https://github.com/google/fhir-data-pipes/issues/1014
    patientDecoded.setText(patient.getText());
    Assert.assertTrue(patient.equalsDeep(patientDecoded));
  }

  private void validateSchema(
      String expectedSchemaFile, List<String> profileResourceTypeUrls, FhirContext fhirContext)
      throws ProfileException, IOException {
    AvroConverter converter = AvroConverter.forResources(fhirContext, profileResourceTypeUrls);
    InputStream inputStream = this.getClass().getResourceAsStream(expectedSchemaFile);
    Schema expectedSchema = new Parser().parse(inputStream);
    Assert.assertEquals(expectedSchema, converter.getSchema());
  }

  private <T extends IBaseResource> IBaseResource loadResource(
      FhirContext fhirContext, String resourceFile, Class<T> resourceType) throws IOException {
    IParser jsonParser = fhirContext.newJsonParser();
    try (InputStream patientStream = getClass().getResourceAsStream(resourceFile)) {
      return jsonParser.parseResource(resourceType, patientStream);
    }
  }
}
