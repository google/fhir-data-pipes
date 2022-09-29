package com.cerner.bunsen.avro;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.stu3.TestData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

// TODO Add unit tests for R4 as well.
public class AvroConverterTest {

  private static final Observation testObservation = TestData.newObservation();

  private static Record avroObservation;

  private static Observation testObservationDecoded;

  private static final Observation testObservationNullStatus = TestData.newObservation()
      .setStatus(Observation.ObservationStatus.NULL);

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
      (MedicationRequest) TestData.newMedicationRequest()
          .addContained(testMedicationOne)
          .addContained(testProvenance)
          .addContained(testMedicationTwo);

  private static Record avroMedicationRequest;

  private static MedicationRequest testMedicationRequestDecoded;

  private static final Patient testBunsenTestProfilePatient = TestData
      .newBunsenTestProfilePatient();

  private static Record avroBunsenTestProfilePatient;

  private static Patient testBunsenTestProfilePatientDecoded;

  /**
   * Initialize test data.
   */
  @BeforeClass
  public static void convertTestData() throws IOException {

    AvroConverter observationConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        "Observation");

    avroObservation = (Record) observationConverter.resourceToAvro(testObservation);

    testObservationDecoded = (Observation) observationConverter.avroToResource(avroObservation);

    avroObservationNullStatus = (Record) observationConverter
        .resourceToAvro(testObservationNullStatus);

    testObservationDecodedNullStatus = (Observation) observationConverter
        .avroToResource(avroObservationNullStatus);

    AvroConverter patientConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_PATIENT);

    avroPatient = (Record) patientConverter.resourceToAvro(testPatient);

    testPatientDecoded = (Patient) patientConverter.avroToResource(avroPatient);

    AvroConverter conditionConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_CONDITION);

    avroCondition = (Record) conditionConverter.resourceToAvro(testCondition);

    testConditionDecoded = (Condition) conditionConverter.avroToResource(avroCondition);

    AvroConverter medicationConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_MEDICATION);

    Record avroMedication = (Record) medicationConverter.resourceToAvro(testMedicationOne);

    testMedicationDecoded = (Medication) medicationConverter.avroToResource(avroMedication);

    AvroConverter medicationRequestConverter = AvroConverter.forResource(FhirContexts.forStu3(),
        TestData.US_CORE_MEDICATION_REQUEST,
        Arrays.asList(TestData.US_CORE_MEDICATION, TestData.PROVENANCE));

    avroMedicationRequest = (Record) medicationRequestConverter
        .resourceToAvro(testMedicationRequest);

    testMedicationRequestDecoded = (MedicationRequest) medicationRequestConverter
        .avroToResource(avroMedicationRequest);

    AvroConverter converterBunsenTestProfilePatient = AvroConverter
        .forResource(FhirContexts.forStu3(), TestData.BUNSEN_TEST_PATIENT);

    avroBunsenTestProfilePatient = (Record) converterBunsenTestProfilePatient
        .resourceToAvro(testBunsenTestProfilePatient);

    testBunsenTestProfilePatientDecoded = (Patient) converterBunsenTestProfilePatient
        .avroToResource(avroBunsenTestProfilePatient);
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Decode the Avro decimal to ensure the expected value is there.
    BigDecimal avroDecimal  = (BigDecimal) ((Record)
        ((Record) avroObservation.get("value"))
        .get("quantity"))
        .get("value");

    Assert.assertEquals(originalDecimal.compareTo(avroDecimal), 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) testObservationDecoded
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void testPrimitiveMultiplicity() {

    Assert.assertTrue(testPatient.getName().get(0).getFamily()
        .equalsIgnoreCase(testPatientDecoded.getName().get(0).getFamily()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(0).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(0).getValueAsString()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(1).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(1).getValueAsString()));
  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));
  }

  /**
   * Tests that FHIR StructureDefinitions that contain fields having identical ChoiceTypes generate
   * an Avro definition that does not trigger an erroneous re-definition of the Avro, and that the
   * converter functions can populate the separate fields even when they share an underlying Avro
   * class for the ChoiceType.
   */
  @Test
  public void testIdenticalChoicesTypes() {

    Assert.assertTrue(testMedicationOne.getIngredientFirstRep()
        .equalsDeep(testMedicationDecoded.getIngredientFirstRep()));

    Assert.assertTrue(testMedicationOne.getPackage().getContentFirstRep()
        .equalsDeep(testMedicationDecoded.getPackage().getContentFirstRep()));
  }

  @Test
  public void testInteger() {

    Integer expectedMultipleBirth = ((IntegerType) testPatient.getMultipleBirth()).getValue();

    Assert.assertEquals(expectedMultipleBirth,
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());

    Assert.assertEquals(expectedMultipleBirth,
        ((Record) avroPatient.get("multipleBirth")).get("integer"));
  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        avroObservation.get("status"));

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
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

    List<Record> severityCodings = (List) ((Record)  avroCondition.get("severity")).get("coding");

    Record severityCoding = severityCodings.get(0);

    Assert.assertEquals(testCoding.getCode(),
        severityCoding.get("code"));
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCoding.get("system"));
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCoding.get("userSelected"));
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCoding.get("display"));
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Record subject = (Record) avroCondition.get("subject");

    Assert.assertEquals(testCondition.getSubject().getReference(),
        subject.get("reference"));

    Assert.assertEquals("12345",  subject.get("PatientId"));

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    Record practitioner = (Record) ((List) avroPatient.get("generalPractitioner")).get(0);

    String organizationId = (String) practitioner.get("OrganizationId");
    String practitionerId = (String) practitioner.get("PractitionerId");

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testSimpleExtension() {

    String testBirthSex = testPatient
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    String decodedBirthSex = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertEquals(testBirthSex, decodedBirthSex);

    Assert.assertEquals(testBirthSex,
        ((Record) avroPatient).get("birthsex"));
  }

  @Test
  public void testNestedExtension() {

    Extension testEthnicity = testPatient
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding testOmbCategory = (Coding) testEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    Coding testDetailed1 = (Coding) testEthnicity
        .getExtensionsByUrl("detailed")
        .get(0)
        .getValue();

    Coding testDetailed2 = (Coding) testEthnicity
        .getExtensionsByUrl("detailed")
        .get(1)
        .getValue();

    String testText = testEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Extension decodedEthnicity = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding decodedOmbCategory = (Coding) decodedEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    Coding decodedDetailed1 = (Coding) decodedEthnicity
        .getExtensionsByUrl("detailed")
        .get(0)
        .getValue();

    Coding decodedDetailed2 = (Coding) decodedEthnicity
        .getExtensionsByUrl("detailed")
        .get(1)
        .getValue();

    String decodedText = decodedEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertTrue(testDetailed1.equalsDeep(decodedDetailed1));
    Assert.assertTrue(testDetailed2.equalsDeep(decodedDetailed2));
    Assert.assertEquals(testText, decodedText);

    Record ethnicityRecord = (Record) avroPatient.get("ethnicity");

    Record ombCategoryRecord =  (Record) ethnicityRecord.get("ombCategory");

    List<Record> detailedRecord =  (List<Record>) ethnicityRecord.get("detailed");

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
    CodeableConcept testMedicationIngredientItem = testMedicationOne.getIngredientFirstRep()
        .getItemCodeableConcept();

    Medication decodedMedicationOne = (Medication) testMedicationRequestDecoded.getContained()
        .get(0);
    String decodedMedicationOneId = decodedMedicationOne.getId();
    CodeableConcept decodedMedicationOneIngredientItem = decodedMedicationOne
        .getIngredientFirstRep()
        .getItemCodeableConcept();

    Assert.assertEquals(testMedicationOneId, decodedMedicationOneId);
    Assert.assertTrue(decodedMedicationOneIngredientItem.equalsDeep(testMedicationIngredientItem));

    Provenance testProvenance = (Provenance) testMedicationRequest.getContained().get(1);
    String testProvenanceId = testProvenance.getId();

    Provenance decodedProvenance = (Provenance) testMedicationRequestDecoded.getContained().get(1);
    String decodedProvenanceId = decodedProvenance.getId();

    Assert.assertEquals(testProvenanceId, decodedProvenanceId);

    Medication testMedicationTwo = (Medication) testMedicationRequest.getContained().get(2);
    String testMedicationTwoId = testMedicationTwo.getId();

    Medication decodedMedicationTwo = (Medication) testMedicationRequestDecoded.getContained()
        .get(2);
    String decodedMedicationTwoId = decodedMedicationTwo.getId();

    Assert.assertEquals(testMedicationTwoId, decodedMedicationTwoId);
  }

  @Test
  public void testCompile() throws IOException {

    List<Schema> schemas = AvroConverter.generateSchemas(FhirContexts.forStu3(),
        ImmutableMap.of(TestData.US_CORE_PATIENT, Collections.emptyList(),
            TestData.VALUE_SET, Collections.emptyList(),
            TestData.US_CORE_MEDICATION_REQUEST, ImmutableList.of(TestData.US_CORE_MEDICATION)));

    // Wrap the schemas in a protocol to simplify the invocation of the compiler.
    Protocol protocol = new Protocol("fhir-test",
        "FHIR Resources for Testing",
        null);

    protocol.setTypes(schemas);

    SpecificCompiler compiler = new SpecificCompiler(protocol);

    Path generatedCodePath = Files.createTempDirectory("generated_code");

    generatedCodePath.toFile().deleteOnExit();

    compiler.compileToDestination(null, generatedCodePath.toFile());

    // Check that java files were created as expected.
    Set<String> javaFiles = Files.find(generatedCodePath,
        10,
        (path, basicFileAttributes) -> true)
        .map(path -> generatedCodePath.relativize(path))
        .map(Object::toString)
        .collect(Collectors.toSet());

    // Ensure common types were generated
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/Period.java"));
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/Coding.java"));
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/ValueSet.java"));

    // The specific profile should be created in the expected sub-package.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/us/core/Patient.java"));

    // Check extension types.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/us/core/UsCoreRace.java"));

    // Choice types include each choice that could be used.
    Assert.assertTrue(javaFiles.contains("com/cerner/bunsen/stu3/avro/ChoiceBooleanInteger.java"));

    // Contained types created.
    Assert.assertTrue(javaFiles.contains(
        "com/cerner/bunsen/stu3/avro/us/core/MedicationRequestContained.java"));
  }

  @Test
  public void testSimpleExtensionWithBooleanField() {

    Boolean expected = (Boolean) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Boolean actual = (Boolean) avroBunsenTestProfilePatient.get("booleanfield");
    Assert.assertEquals(expected, actual);

    Boolean decodedBooleanField = (Boolean) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedBooleanField);
  }

  @Test
  public void testSimpleExtensionWithIntegerField() {

    Integer expected = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Integer actual = (Integer) avroBunsenTestProfilePatient.get("integerfield");
    Assert.assertEquals(expected, actual);

    Integer decodedIntegerField = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedIntegerField);
  }

  @Test
  public void testMultiExtensionWithIntegerArrayField() {

    Integer expected1 = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Integer expected2 = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(1).getValueAsPrimitive().getValue();

    Integer actual1 = ((List<Integer>)avroBunsenTestProfilePatient.get("integerArrayField")).get(0);
    Integer actual2 = ((List<Integer>)avroBunsenTestProfilePatient.get("integerArrayField")).get(1);

    Assert.assertEquals(expected1, actual1);
    Assert.assertEquals(expected2, actual2);

    Integer decodedIntegerField1 = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Integer decodedIntegerField2 = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
        .get(1).getValueAsPrimitive().getValue();

    Assert.assertEquals(expected1, decodedIntegerField1);
    Assert.assertEquals(expected2, decodedIntegerField2);

    final List<Record> nestedExtList = (List<Record>) avroBunsenTestProfilePatient
        .get("nestedExt");
  }

  @Test
  public void testMultiNestedExtension() {

    final Extension nestedExtension1 = testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(0);

    final Extension nestedExtension2 = testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(1);

    String text1 = nestedExtension1.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    String text2 = nestedExtension1.getExtensionsByUrl("text")
        .get(1).getValueAsPrimitive().getValueAsString();

    String text3 = nestedExtension2.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    CodeableConcept codeableConcept1 = (CodeableConcept) nestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept codeableConcept2 = (CodeableConcept) nestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept codeableConcept3 = (CodeableConcept) nestedExtension2
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    final Extension decodedNestedExtension1 = testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(0);

    final Extension decodedNestedExtension2 = testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
        .get(1);

    String decodedText1 = decodedNestedExtension1.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    String decodedText2 = decodedNestedExtension1.getExtensionsByUrl("text")
        .get(1).getValueAsPrimitive().getValueAsString();

    String decodedText3 = decodedNestedExtension2.getExtensionsByUrl("text")
        .get(0).getValueAsPrimitive().getValueAsString();

    CodeableConcept decodedCodeableConcept1 = (CodeableConcept) decodedNestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept decodedCodeableConcept2 = (CodeableConcept) decodedNestedExtension1
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept decodedCodeableConcept3 = (CodeableConcept) decodedNestedExtension2
        .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
        .get(0).getValue();

    Assert.assertEquals(text1, decodedText1);
    Assert.assertEquals(text2, decodedText2);
    Assert.assertEquals(text3, decodedText3);

    Assert.assertTrue(codeableConcept1.equalsDeep(decodedCodeableConcept1));
    Assert.assertTrue(codeableConcept2.equalsDeep(decodedCodeableConcept2));
    Assert.assertTrue(codeableConcept3.equalsDeep(decodedCodeableConcept3));

    final List<Record> nestedExtList = (List<Record>) avroBunsenTestProfilePatient
        .get("nestedExt");

    final Record nestedExt1 = nestedExtList.get(0);
    final Record nestedExt2 = nestedExtList.get(1);

    final List<Record> textList1 = (List<Record>) nestedExt1.get("text");
    final List<Record> textList2 = (List<Record>) nestedExt2.get("text");

    final List<Record> codeableConceptsList1 = (List<Record>) nestedExt1.get("codeableConceptExt");
    final List<Record> codeableConceptsList2 = (List<Record>) nestedExt2.get("codeableConceptExt");

    Assert.assertEquals(text1, textList1.get(0));
    Assert.assertEquals(text2, textList1.get(1));
    Assert.assertEquals(text3, textList2.get(0));

    Assert.assertEquals(codeableConcept1.getCoding().get(0).getCode(),
        ((List<Record>)codeableConceptsList1.get(0).get("coding")).get(0).get("code"));

    Assert.assertEquals(codeableConcept2.getCoding().get(0).getCode(),
        ((List<Record>)codeableConceptsList1.get(1).get("coding")).get(0).get("code"));

    Assert.assertEquals(codeableConcept3.getCoding().get(0).getCode(),
        ((List<Record>)codeableConceptsList2.get(0).get("coding")).get(0).get("code"));
  }

  @Test
  public void testSimpleModifierExtensionWithStringField() {

    String expected = (String) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    String actual = (String) avroBunsenTestProfilePatient.get("stringModifierExt");

    Assert.assertEquals(expected, actual);

    String decodedStringField = (String) testBunsenTestProfilePatientDecoded
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
        .get(0).getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedStringField);
  }

  @Test
  public void testMultiModifierExtensionsWithCodeableConceptField() {

    CodeableConcept expected1 = (CodeableConcept) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
        .get(0).getValue();

    CodeableConcept expected2 = (CodeableConcept) testBunsenTestProfilePatient
        .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
        .get(1).getValue();

    CodeableConcept decodedCodeableConceptField1 =
        (CodeableConcept) testBunsenTestProfilePatientDecoded
            .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
            .get(0).getValue();

    CodeableConcept decodedCodeableConceptField2 =
        (CodeableConcept) testBunsenTestProfilePatientDecoded
            .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
            .get(1).getValue();

    Assert.assertTrue(expected1.equalsDeep(decodedCodeableConceptField1));
    Assert.assertTrue(expected2.equalsDeep(decodedCodeableConceptField2));

    final List<Record> codeableConceptList = (List<Record>) avroBunsenTestProfilePatient
        .get("codeableConceptModifierExt");

    final Record codeableConcept1 = codeableConceptList.get(0);
    final Record codeableConcept2 = codeableConceptList.get(1);

    Assert.assertEquals(decodedCodeableConceptField1.getCoding().get(0).getSystem(),
        ((List<Record>)codeableConcept1.get("coding")).get(0).get("system"));
    Assert.assertEquals(decodedCodeableConceptField1.getCoding().get(0).getCode(),
        ((List<Record>)codeableConcept1.get("coding")).get(0).get("code"));
    Assert.assertEquals(decodedCodeableConceptField1.getCoding().get(0).getDisplay(),
        ((List<Record>)codeableConcept1.get("coding")).get(0).get("display"));

    Assert.assertEquals(decodedCodeableConceptField2.getCoding().get(0).getSystem(),
        ((List<Record>)codeableConcept2.get("coding")).get(0).get("system"));
    Assert.assertEquals(decodedCodeableConceptField2.getCoding().get(0).getCode(),
        ((List<Record>)codeableConcept2.get("coding")).get(0).get("code"));
    Assert.assertEquals(decodedCodeableConceptField2.getCoding().get(0).getDisplay(),
        ((List<Record>)codeableConcept2.get("coding")).get(0).get("display"));
  }

  @Test
  public void testMetaElement() {

    String id =  testPatient.getId();
    Meta meta = testPatient.getMeta();

    Assert.assertEquals(id, testPatientDecoded.getId());

    Assert.assertEquals(meta.getTag().size(), testPatientDecoded.getMeta().getTag().size());
    Assert.assertEquals(meta.getTag().get(0).getCode(),
        testPatientDecoded.getMeta().getTag().get(0).getCode());
    Assert.assertEquals(meta.getTag().get(0).getSystem(),
        testPatientDecoded.getMeta().getTag().get(0).getSystem());
  }
}
