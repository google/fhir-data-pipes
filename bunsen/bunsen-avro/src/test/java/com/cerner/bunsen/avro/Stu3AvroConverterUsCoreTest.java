package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.common.Stu3UsCoreProfileData;
import com.cerner.bunsen.exception.ProfileException;
import com.cerner.bunsen.stu3.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData.Record;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Medication;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.Meta;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Provenance;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class Stu3AvroConverterUsCoreTest {

  private static final Observation testObservation = TestData.newObservation();

  private static Record avroObservation;

  private static Observation testObservationDecoded;

  private static final Observation testObservationNullStatus =
      TestData.newObservation().setStatus(Observation.ObservationStatus.NULL);

  private static Record avroObservationNullStatus;

  private static Observation testObservationDecodedNullStatus;

  private static final Patient testPatient = TestData.newPatient();

  private static Record avroPatient;

  private static Patient testPatientDecoded;

  private static final Condition testCondition = TestData.newCondition();

  private static Record avroCondition;

  private static Condition testConditionDecoded;

  private static final Medication testMedicationOne = TestData.newMedication("test-medication-1");

  private static final Medication testMedicationTwo = TestData.newMedication("test-medication-2");

  private static Medication testMedicationDecoded;

  private static final Provenance testProvenance = TestData.newProvenance();

  private static final MedicationRequest testMedicationRequest =
      (MedicationRequest)
          TestData.newMedicationRequest()
              .addContained(testMedicationOne)
              .addContained(testProvenance)
              .addContained(testMedicationTwo);

  private static Record avroMedicationRequest;

  private static MedicationRequest testMedicationRequestDecoded;

  private static FhirContext fhirContext;

  /** Initialize test data. */
  @BeforeClass
  public static void convertTestData() throws ProfileException {
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.DSTU3);
    fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFor(FhirVersionEnum.DSTU3, "classpath:/stu3-us-core-definitions");
    AvroConverter observationConverter =
        AvroConverter.forResources(fhirContext, Stu3UsCoreProfileData.US_CORE_OBSERVATION_PROFILES);

    avroObservation = (Record) observationConverter.resourceToAvro(testObservation);

    testObservationDecoded = (Observation) observationConverter.avroToResource(avroObservation);

    avroObservationNullStatus =
        (Record) observationConverter.resourceToAvro(testObservationNullStatus);

    testObservationDecodedNullStatus =
        (Observation) observationConverter.avroToResource(avroObservationNullStatus);

    AvroConverter patientConverter =
        AvroConverter.forResources(fhirContext, Stu3UsCoreProfileData.US_CORE_PATIENT_PROFILES);

    avroPatient = (Record) patientConverter.resourceToAvro(testPatient);

    testPatientDecoded = (Patient) patientConverter.avroToResource(avroPatient);

    AvroConverter conditionConverter =
        AvroConverter.forResources(fhirContext, Stu3UsCoreProfileData.US_CORE_CONDITION_PROFILES);

    avroCondition = (Record) conditionConverter.resourceToAvro(testCondition);

    testConditionDecoded = (Condition) conditionConverter.avroToResource(avroCondition);

    AvroConverter medicationConverter =
        AvroConverter.forResources(fhirContext, Stu3UsCoreProfileData.US_CORE_MEDICATION_PROFILES);

    Record avroMedication = (Record) medicationConverter.resourceToAvro(testMedicationOne);

    testMedicationDecoded = (Medication) medicationConverter.avroToResource(avroMedication);

    // TODO: Contained resources are not supported yet for multiple profiles
    AvroConverter medicationRequestConverter =
        AvroConverter.forResource(
            fhirContext,
            TestData.US_CORE_MEDICATION_REQUEST,
            Arrays.asList(TestData.US_CORE_MEDICATION, TestData.PROVENANCE));

    avroMedicationRequest =
        (Record) medicationRequestConverter.resourceToAvro(testMedicationRequest);

    testMedicationRequestDecoded =
        (MedicationRequest) medicationRequestConverter.avroToResource(avroMedicationRequest);
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Decode the Avro decimal to ensure the expected value is there.
    BigDecimal avroDecimal =
        BigDecimal.valueOf(
            (Double)
                ((Record) ((Record) avroObservation.get("value")).get("quantity")).get("value"));

    Assert.assertEquals(originalDecimal.compareTo(avroDecimal), 0);

    Assert.assertEquals(
        originalDecimal.compareTo(((Quantity) testObservationDecoded.getValue()).getValue()), 0);
  }

  @Test
  public void testPrimitiveMultiplicity() {

    Assert.assertTrue(
        testPatient
            .getName()
            .get(0)
            .getFamily()
            .equalsIgnoreCase(testPatientDecoded.getName().get(0).getFamily()));
    Assert.assertTrue(
        testPatient
            .getName()
            .get(0)
            .getGiven()
            .get(0)
            .getValueAsString()
            .equals(testPatientDecoded.getName().get(0).getGiven().get(0).getValueAsString()));
    Assert.assertTrue(
        testPatient
            .getName()
            .get(0)
            .getGiven()
            .get(1)
            .getValueAsString()
            .equals(testPatientDecoded.getName().get(0).getGiven().get(1).getValueAsString()));
  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(
        testPatient.getMultipleBirth().equalsDeep(testPatientDecoded.getMultipleBirth()));
  }

  @Test
  public void testIdInNestedElement() throws FHIRException {

    // Ensure that nested elements do not have id as property.
    Assert.assertNull(testPatientDecoded.getAddress().get(0).getId());
    Assert.assertNull(testPatientDecoded.getName().get(0).getId());
  }

  /**
   * Tests that FHIR StructureDefinitions that contain fields having identical ChoiceTypes generate
   * an Avro definition that does not trigger an erroneous re-definition of the Avro, and that the
   * converter functions can populate the separate fields even when they share an underlying Avro
   * class for the ChoiceType.
   */
  @Test
  public void testIdenticalChoicesTypes() {

    Assert.assertTrue(
        testMedicationOne
            .getIngredientFirstRep()
            .equalsDeep(testMedicationDecoded.getIngredientFirstRep()));

    Assert.assertTrue(
        testMedicationOne
            .getPackage()
            .getContentFirstRep()
            .equalsDeep(testMedicationDecoded.getPackage().getContentFirstRep()));
  }

  @Test
  public void testInteger() {

    Integer expectedMultipleBirth = ((IntegerType) testPatient.getMultipleBirth()).getValue();

    Assert.assertEquals(
        expectedMultipleBirth, ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());

    Assert.assertEquals(
        expectedMultipleBirth, ((Record) avroPatient.get("multipleBirth")).get("integer"));
  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(), avroObservation.get("status"));

    Assert.assertEquals(testObservation.getStatus(), testObservationDecoded.getStatus());
  }

  @Test
  public void testBoundCodeNull() {

    Assert.assertNull(avroObservationNullStatus.get("status"));

    Assert.assertNull(testObservationDecodedNullStatus.getStatusElement().getValue());
  }

  @Test
  public void testCoding() {

    Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    List<Record> severityCodings = (List) ((Record) avroCondition.get("severity")).get("coding");

    Record severityCoding = severityCodings.get(0);

    Assert.assertEquals(testCoding.getCode(), severityCoding.get("code"));
    Assert.assertEquals(testCoding.getCode(), decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(), severityCoding.get("system"));
    Assert.assertEquals(testCoding.getSystem(), decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(), severityCoding.get("userSelected"));
    Assert.assertEquals(testCoding.getUserSelected(), decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(), severityCoding.get("display"));
    Assert.assertEquals(testCoding.getDisplay(), decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Record subject = (Record) avroCondition.get("subject");

    Assert.assertEquals(testCondition.getSubject().getReference(), subject.get("reference"));

    Assert.assertEquals("12345", subject.get("patientId"));

    Assert.assertEquals(
        testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testManagingOrganizationIdentifier() {

    Identifier identifier = testPatientDecoded.getManagingOrganization().getIdentifier();

    Assert.assertNotNull(identifier);
    Assert.assertEquals(identifier.getAssigner().getReference(), "Organization/234");
  }

  @Test
  public void testMultiReferenceTypes() {

    Record practitioner = (Record) ((List) avroPatient.get("generalPractitioner")).get(0);

    String organizationId = (String) practitioner.get("organizationId");
    String practitionerId = (String) practitioner.get("practitionerId");

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(
        testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(
        testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testSimpleExtension() {

    String testBirthSex =
        testPatient
            .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
            .get(0)
            .getValueAsPrimitive()
            .getValueAsString();

    String decodedBirthSex =
        testPatientDecoded
            .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
            .get(0)
            .getValueAsPrimitive()
            .getValueAsString();

    Assert.assertEquals(testBirthSex, decodedBirthSex);

    Assert.assertEquals(testBirthSex, ((Record) avroPatient).get("birthsex"));
  }

  @Test
  public void testNestedExtension() {

    Extension testEthnicity = testPatient.getExtensionsByUrl(TestData.US_CORE_ETHNICITY).get(0);

    Coding testOmbCategory =
        (Coding) testEthnicity.getExtensionsByUrl("ombCategory").get(0).getValue();

    Coding testDetailed1 = (Coding) testEthnicity.getExtensionsByUrl("detailed").get(0).getValue();

    Coding testDetailed2 = (Coding) testEthnicity.getExtensionsByUrl("detailed").get(1).getValue();

    String testText =
        testEthnicity.getExtensionsByUrl("text").get(0).getValueAsPrimitive().getValueAsString();

    Extension decodedEthnicity =
        testPatientDecoded.getExtensionsByUrl(TestData.US_CORE_ETHNICITY).get(0);

    Coding decodedOmbCategory =
        (Coding) decodedEthnicity.getExtensionsByUrl("ombCategory").get(0).getValue();

    Coding decodedDetailed1 =
        (Coding) decodedEthnicity.getExtensionsByUrl("detailed").get(0).getValue();

    Coding decodedDetailed2 =
        (Coding) decodedEthnicity.getExtensionsByUrl("detailed").get(1).getValue();

    String decodedText =
        decodedEthnicity.getExtensionsByUrl("text").get(0).getValueAsPrimitive().getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertTrue(testDetailed1.equalsDeep(decodedDetailed1));
    Assert.assertTrue(testDetailed2.equalsDeep(decodedDetailed2));
    Assert.assertEquals(testText, decodedText);

    Record ethnicityRecord = (Record) avroPatient.get("ethnicity");

    Record ombCategoryRecord = (Record) ethnicityRecord.get("ombCategory");

    List<Record> detailedRecord = (List<Record>) ethnicityRecord.get("detailed");

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRecord.get("system"));
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRecord.get("code"));
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRecord.get("display"));

    Assert.assertEquals(testDetailed1.getSystem(), detailedRecord.get(0).get("system"));
    Assert.assertEquals(testDetailed1.getCode(), detailedRecord.get(0).get("code"));
    Assert.assertEquals(testDetailed1.getDisplay(), detailedRecord.get(0).get("display"));

    Assert.assertEquals(testDetailed2.getSystem(), detailedRecord.get(1).get("system"));
    Assert.assertEquals(testDetailed2.getCode(), detailedRecord.get(1).get("code"));
    Assert.assertEquals(testDetailed2.getDisplay(), detailedRecord.get(1).get("display"));

    Assert.assertEquals(testText, ethnicityRecord.get("text"));
  }

  @Test
  public void testContainedResources() throws FHIRException {

    Medication testMedicationOne = (Medication) testMedicationRequest.getContained().get(0);
    String testMedicationOneId = testMedicationOne.getId();
    CodeableConcept testMedicationIngredientItem =
        testMedicationOne.getIngredientFirstRep().getItemCodeableConcept();

    Medication decodedMedicationOne =
        (Medication) testMedicationRequestDecoded.getContained().get(0);
    String decodedMedicationOneId = decodedMedicationOne.getId();
    CodeableConcept decodedMedicationOneIngredientItem =
        decodedMedicationOne.getIngredientFirstRep().getItemCodeableConcept();

    Assert.assertEquals(testMedicationOneId, decodedMedicationOneId);
    Assert.assertTrue(decodedMedicationOneIngredientItem.equalsDeep(testMedicationIngredientItem));

    Provenance testProvenance = (Provenance) testMedicationRequest.getContained().get(1);
    String testProvenanceId = testProvenance.getId();

    Provenance decodedProvenance = (Provenance) testMedicationRequestDecoded.getContained().get(1);
    String decodedProvenanceId = decodedProvenance.getId();

    Assert.assertEquals(testProvenanceId, decodedProvenanceId);

    Medication testMedicationTwo = (Medication) testMedicationRequest.getContained().get(2);
    String testMedicationTwoId = testMedicationTwo.getId();
    String testMedicationTwoReference =
        testMedicationTwo.getPackage().getContent().get(0).getItemReference().getReference();

    Medication decodedMedicationTwo =
        (Medication) testMedicationRequestDecoded.getContained().get(2);
    String decodedMedicationTwoId = decodedMedicationTwo.getId();
    String decodedMedicationTwoReference =
        decodedMedicationTwo.getPackage().getContent().get(0).getItemReference().getReference();

    Assert.assertEquals(testMedicationTwoId, decodedMedicationTwoId);
    Assert.assertEquals(testMedicationTwoReference, decodedMedicationTwoReference);
  }

  @Test
  public void testCompile() throws IOException {

    List<Schema> schemas =
        AvroConverter.generateSchemas(
            fhirContext,
            ImmutableMap.of(
                TestData.US_CORE_PATIENT,
                Collections.emptyList(),
                TestData.VALUE_SET,
                Collections.emptyList(),
                TestData.US_CORE_MEDICATION_REQUEST,
                ImmutableList.of(TestData.US_CORE_MEDICATION)));

    // Wrap the schemas in a protocol to simplify the invocation of the compiler.
    Protocol protocol = new Protocol("fhir-test", "FHIR Resources for Testing", null);

    protocol.setTypes(schemas);

    SpecificCompiler compiler = new SpecificCompiler(protocol);

    Path generatedCodePath = Files.createTempDirectory("generated_code");

    generatedCodePath.toFile().deleteOnExit();

    compiler.compileToDestination(null, generatedCodePath.toFile());

    // Check that java files were created as expected.
    Set<String> javaFiles =
        Files.find(generatedCodePath, 10, (path, basicFileAttributes) -> true)
            .map(path -> generatedCodePath.relativize(path))
            .map(Object::toString)
            .collect(Collectors.toSet());

    String fileSeparator = File.separator;
    List<String> filesToBeVerified =
        Arrays.asList(
            // Ensure common types were generated
            String.join(
                fileSeparator,
                new String[] {"com", "cerner", "bunsen", "stu3", "avro", "Period.java"}),
            String.join(
                fileSeparator,
                new String[] {"com", "cerner", "bunsen", "stu3", "avro", "PatientCoding.java"}),
            String.join(
                fileSeparator,
                new String[] {"com", "cerner", "bunsen", "stu3", "avro", "ValueSet.java"}),
            // The specific profile should be created in the expected sub-package.
            String.join(
                fileSeparator,
                new String[] {
                  "com", "cerner", "bunsen", "stu3", "avro", "us", "core", "Patient.java"
                }),
            // Check extension types.
            String.join(
                fileSeparator,
                new String[] {
                  "com", "cerner", "bunsen", "stu3", "avro", "us", "core", "UsCoreRace.java"
                }),
            // Choice types include each choice that could be used.
            String.join(
                fileSeparator,
                new String[] {
                  "com", "cerner", "bunsen", "stu3", "avro", "ChoiceBooleanInteger.java"
                }),
            // Contained types created.
            String.join(
                fileSeparator,
                new String[] {
                  "com",
                  "cerner",
                  "bunsen",
                  "stu3",
                  "avro",
                  "us",
                  "core",
                  "MedicationRequestContained.java"
                }));

    // Ensure common types were generated
    for (String fileToBeVerified : filesToBeVerified) {
      Assert.assertTrue(javaFiles.contains(fileToBeVerified));
    }
  }

  @Test
  public void testMetaElement() {

    String id = testPatient.getId();
    Meta meta = testPatient.getMeta();

    Assert.assertEquals(id, testPatientDecoded.getId());

    Assert.assertEquals(meta.getTag().size(), testPatientDecoded.getMeta().getTag().size());
    Assert.assertEquals(
        meta.getTag().get(0).getCode(), testPatientDecoded.getMeta().getTag().get(0).getCode());
    Assert.assertEquals(
        meta.getTag().get(0).getSystem(), testPatientDecoded.getMeta().getTag().get(0).getSystem());
  }
}
