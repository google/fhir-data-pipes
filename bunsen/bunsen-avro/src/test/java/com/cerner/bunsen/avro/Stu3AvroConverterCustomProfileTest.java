package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.exception.ProfileException;
import com.cerner.bunsen.stu3.TestData;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericData.Record;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Patient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the used defined structure definitions which should also follow the HL7 FHIR
 * specifications and these are just some additional test cases, apart from the regular US Core
 * Profiles tests.
 *
 * <p>TODO: Refactor this and the R43AvroConverterCustomProfileTest.java to move the duplicate code
 * into a common class and add only relevant cases here
 */
public class Stu3AvroConverterCustomProfileTest {

  private static final Patient testBunsenTestProfilePatient =
      TestData.newBunsenTestProfilePatient();
  private static Record avroBunsenTestProfilePatient;

  private static Patient testBunsenTestProfilePatientDecoded;

  @BeforeClass
  public static void setUp() throws URISyntaxException, ProfileException {
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(FhirVersionEnum.DSTU3);
    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFor(FhirVersionEnum.DSTU3, "classpath:/stu3-custom-profile-definitions");
    List<String> patientProfiles =
        Arrays.asList(
            "http://hl7.org/fhir/StructureDefinition/Patient",
            "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient");
    AvroConverter converterBunsenTestProfilePatient =
        AvroConverter.forResources(fhirContext, patientProfiles, 1);

    avroBunsenTestProfilePatient =
        (Record) converterBunsenTestProfilePatient.resourceToAvro(testBunsenTestProfilePatient);

    testBunsenTestProfilePatientDecoded =
        (Patient) converterBunsenTestProfilePatient.avroToResource(avroBunsenTestProfilePatient);
  }

  @Test
  public void testSimpleExtensionWithBooleanField() {

    Boolean expected =
        (Boolean)
            testBunsenTestProfilePatient
                .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Boolean actual = (Boolean) avroBunsenTestProfilePatient.get("booleanfield");
    Assert.assertEquals(expected, actual);

    Boolean decodedBooleanField =
        (Boolean)
            testBunsenTestProfilePatientDecoded
                .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Assert.assertEquals(expected, decodedBooleanField);
  }

  @Test
  public void testSimpleExtensionWithIntegerField() {

    Integer expected =
        (Integer)
            testBunsenTestProfilePatient
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Integer actual = (Integer) avroBunsenTestProfilePatient.get("integerfield");
    Assert.assertEquals(expected, actual);

    Integer decodedIntegerField =
        (Integer)
            testBunsenTestProfilePatientDecoded
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Assert.assertEquals(expected, decodedIntegerField);
  }

  @Test
  public void testMultiExtensionWithIntegerArrayField() {

    Integer expected1 =
        (Integer)
            testBunsenTestProfilePatient
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Integer expected2 =
        (Integer)
            testBunsenTestProfilePatient
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
                .get(1)
                .getValueAsPrimitive()
                .getValue();

    Integer actual1 =
        ((List<Integer>) avroBunsenTestProfilePatient.get("integerArrayField")).get(0);
    Integer actual2 =
        ((List<Integer>) avroBunsenTestProfilePatient.get("integerArrayField")).get(1);

    Assert.assertEquals(expected1, actual1);
    Assert.assertEquals(expected2, actual2);

    Integer decodedIntegerField1 =
        (Integer)
            testBunsenTestProfilePatientDecoded
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Integer decodedIntegerField2 =
        (Integer)
            testBunsenTestProfilePatientDecoded
                .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_ARRAY_FIELD)
                .get(1)
                .getValueAsPrimitive()
                .getValue();

    Assert.assertEquals(expected1, decodedIntegerField1);
    Assert.assertEquals(expected2, decodedIntegerField2);
  }

  @Test
  public void testMultiNestedExtension() {

    final Extension nestedExtension1 =
        testBunsenTestProfilePatient
            .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
            .get(0);

    final Extension nestedExtension2 =
        testBunsenTestProfilePatient
            .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
            .get(1);

    String text1 =
        nestedExtension1.getExtensionsByUrl("text").get(0).getValueAsPrimitive().getValueAsString();

    String text2 =
        nestedExtension1.getExtensionsByUrl("text").get(1).getValueAsPrimitive().getValueAsString();

    String text3 =
        nestedExtension2.getExtensionsByUrl("text").get(0).getValueAsPrimitive().getValueAsString();

    CodeableConcept codeableConcept1 =
        (CodeableConcept)
            nestedExtension1
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(0)
                .getValue();

    CodeableConcept codeableConcept2 =
        (CodeableConcept)
            nestedExtension1
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(1)
                .getValue();

    CodeableConcept codeableConcept3 =
        (CodeableConcept)
            nestedExtension2
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(0)
                .getValue();

    final Extension decodedNestedExtension1 =
        testBunsenTestProfilePatientDecoded
            .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
            .get(0);

    final Extension decodedNestedExtension2 =
        testBunsenTestProfilePatientDecoded
            .getExtensionsByUrl(TestData.BUNSEN_TEST_NESTED_EXT_FIELD)
            .get(1);

    String decodedText1 =
        decodedNestedExtension1
            .getExtensionsByUrl("text")
            .get(0)
            .getValueAsPrimitive()
            .getValueAsString();

    String decodedText2 =
        decodedNestedExtension1
            .getExtensionsByUrl("text")
            .get(1)
            .getValueAsPrimitive()
            .getValueAsString();

    String decodedText3 =
        decodedNestedExtension2
            .getExtensionsByUrl("text")
            .get(0)
            .getValueAsPrimitive()
            .getValueAsString();

    CodeableConcept decodedCodeableConcept1 =
        (CodeableConcept)
            decodedNestedExtension1
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(0)
                .getValue();

    CodeableConcept decodedCodeableConcept2 =
        (CodeableConcept)
            decodedNestedExtension1
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(1)
                .getValue();

    CodeableConcept decodedCodeableConcept3 =
        (CodeableConcept)
            decodedNestedExtension2
                .getExtensionsByUrl(TestData.BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD)
                .get(0)
                .getValue();

    Assert.assertEquals(text1, decodedText1);
    Assert.assertEquals(text2, decodedText2);
    Assert.assertEquals(text3, decodedText3);

    Assert.assertTrue(codeableConcept1.equalsDeep(decodedCodeableConcept1));
    Assert.assertTrue(codeableConcept2.equalsDeep(decodedCodeableConcept2));
    Assert.assertTrue(codeableConcept3.equalsDeep(decodedCodeableConcept3));

    final List<Record> nestedExtList = (List<Record>) avroBunsenTestProfilePatient.get("nestedExt");

    final Record nestedExt1 = nestedExtList.get(0);
    final Record nestedExt2 = nestedExtList.get(1);

    final List<Record> textList1 = (List<Record>) nestedExt1.get("text");
    final List<Record> textList2 = (List<Record>) nestedExt2.get("text");

    final List<Record> codeableConceptsList1 = (List<Record>) nestedExt1.get("codeableConceptExt");
    final List<Record> codeableConceptsList2 = (List<Record>) nestedExt2.get("codeableConceptExt");

    Assert.assertEquals(text1, textList1.get(0).toString());
    Assert.assertEquals(text2, textList1.get(1).toString());
    Assert.assertEquals(text3, textList2.get(0).toString());

    Assert.assertEquals(
        codeableConcept1.getCoding().get(0).getCode(),
        ((List<Record>) codeableConceptsList1.get(0).get("coding")).get(0).get("code"));

    Assert.assertEquals(
        codeableConcept2.getCoding().get(0).getCode(),
        ((List<Record>) codeableConceptsList1.get(1).get("coding")).get(0).get("code"));

    Assert.assertEquals(
        codeableConcept3.getCoding().get(0).getCode(),
        ((List<Record>) codeableConceptsList2.get(0).get("coding")).get(0).get("code"));
  }

  @Test
  public void testSimpleModifierExtensionWithStringField() {

    String expected =
        (String)
            testBunsenTestProfilePatient
                .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    String actual = (String) avroBunsenTestProfilePatient.get("stringModifierExt");

    Assert.assertEquals(expected, actual);

    String decodedStringField =
        (String)
            testBunsenTestProfilePatientDecoded
                .getModifierExtensionsByUrl(TestData.BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD)
                .get(0)
                .getValueAsPrimitive()
                .getValue();

    Assert.assertEquals(expected, decodedStringField);
  }

  @Test
  public void testMultiModifierExtensionsWithCodeableConceptField() {

    CodeableConcept expected1 =
        (CodeableConcept)
            testBunsenTestProfilePatient
                .getModifierExtensionsByUrl(
                    TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
                .get(0)
                .getValue();

    CodeableConcept expected2 =
        (CodeableConcept)
            testBunsenTestProfilePatient
                .getModifierExtensionsByUrl(
                    TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
                .get(1)
                .getValue();

    CodeableConcept decodedCodeableConceptField1 =
        (CodeableConcept)
            testBunsenTestProfilePatientDecoded
                .getModifierExtensionsByUrl(
                    TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
                .get(0)
                .getValue();

    CodeableConcept decodedCodeableConceptField2 =
        (CodeableConcept)
            testBunsenTestProfilePatientDecoded
                .getModifierExtensionsByUrl(
                    TestData.BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD)
                .get(1)
                .getValue();

    Assert.assertTrue(expected1.equalsDeep(decodedCodeableConceptField1));
    Assert.assertTrue(expected2.equalsDeep(decodedCodeableConceptField2));

    final List<Record> codeableConceptList =
        (List<Record>) avroBunsenTestProfilePatient.get("codeableConceptModifierExt");

    final Record codeableConcept1 = codeableConceptList.get(0);
    final Record codeableConcept2 = codeableConceptList.get(1);

    Assert.assertEquals(
        decodedCodeableConceptField1.getCoding().get(0).getSystem(),
        ((List<Record>) codeableConcept1.get("coding")).get(0).get("system"));
    Assert.assertEquals(
        decodedCodeableConceptField1.getCoding().get(0).getCode(),
        ((List<Record>) codeableConcept1.get("coding")).get(0).get("code"));
    Assert.assertEquals(
        decodedCodeableConceptField1.getCoding().get(0).getDisplay(),
        ((List<Record>) codeableConcept1.get("coding")).get(0).get("display"));

    Assert.assertEquals(
        decodedCodeableConceptField2.getCoding().get(0).getSystem(),
        ((List<Record>) codeableConcept2.get("coding")).get(0).get("system"));
    Assert.assertEquals(
        decodedCodeableConceptField2.getCoding().get(0).getCode(),
        ((List<Record>) codeableConcept2.get("coding")).get(0).get("code"));
    Assert.assertEquals(
        decodedCodeableConceptField2.getCoding().get(0).getDisplay(),
        ((List<Record>) codeableConcept2.get("coding")).get(0).get("display"));
  }
}
