package com.cerner.bunsen.r4;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Dosage;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.Medication.MedicationIngredientComponent;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.MedicationRequest.MedicationRequestSubstitutionComponent;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Narrative;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Provenance;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Timing;
import org.hl7.fhir.r4.model.Timing.TimingRepeatComponent;
import org.hl7.fhir.utilities.xhtml.NodeType;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;


/**
 * Common test resources for Bunsen R4 usage.
 */
public class TestData {

  public static final String US_CORE_BIRTHSEX
      = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex";

  public static final String US_CORE_ETHNICITY
      = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity";

  public static final String US_CORE_PATIENT =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

  public static final String US_CORE_OBSERVATION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observationresults";

  public static final String US_CORE_CONDITION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition";

  public static final String US_CORE_MEDICATION =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medication";

  public static final String US_CORE_MEDICATION_REQUEST =
      "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medicationrequest";

  public static final String PROVENANCE =
      "http://hl7.org/fhir/StructureDefinition/Provenance";

  public static final String VALUE_SET =
      "http://hl7.org/fhir/StructureDefinition/ValueSet";

  public static final String BUNSEN_TEST_PATIENT =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-patient";

  public static final String BUNSEN_TEST_BOOLEAN_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-booleanfield";

  public static final String BUNSEN_TEST_INTEGER_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-integerfield";

  public static final String BUNSEN_TEST_INTEGER_ARRAY_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-integerArrayField";

  public static final String BUNSEN_TEST_NESTED_EXT_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-nested-extension";

  public static final String BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-codeableConcept-extension";

  public static final String BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/"
          + "bunsen-test-codeableConcept-modifierExtension";

  public static final String BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD =
      "http://hl7.org/fhir/bunsen/test/StructureDefinition/bunsen-test-string-modifierExtension";

  /**
   * Returns a FHIR Condition for testing purposes.
   *
   * @return a FHIR Condition for testing.
   */
  public static Condition newCondition() {

    Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId("Condition/example");

    condition.setLanguage("en_US");

    // Narrative text
    Narrative narrative = new Narrative();
    narrative.setStatusAsString("generated");
    narrative.setDivAsString("This data was generated for test purposes.");
    XhtmlNode node = new XhtmlNode();
    node.setNodeType(NodeType.Text);
    node.setValue("Severe burn of left ear (Date: 24-May 2012)");
    condition.setText(narrative);

    condition.setSubject(new Reference("Patient/12345").setDisplay("Here is a display for you."));

    // TODO
    // condition.setVerificationStatus(Condition.ConditionVerificationStatus.CONFIRMED);
    CodeableConcept verStatus = new CodeableConcept();
    verStatus.addCoding().setSystem("http://terminology.hl7.org/CodeSystem/condition-ver-status")
            .setCode("confirmed");
    condition.setVerificationStatus(verStatus);

    // Condition code
    CodeableConcept code = new CodeableConcept();
    code.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("39065001")
        .setDisplay("Severe");
    condition.setSeverity(code);

    // Severity code
    CodeableConcept severity = new CodeableConcept();
    severity.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("24484000")
        .setDisplay("Burn of ear")
        .setUserSelected(true);
    condition.setSeverity(severity);

    // Onset date time
    DateTimeType onset = new DateTimeType();
    onset.setValueAsString("2012-05-24");
    condition.setOnset(onset);

    return condition;
  }

  /**
   * Returns a new Observation for testing.
   *
   * @return a FHIR Observation for testing.
   */
  public static Observation newObservation() {
    Observation observation = new Observation();

    observation.setId("blood-pressure");

    Identifier identifier = observation.addIdentifier();
    identifier.setSystem("urn:ietf:rfc:3986");
    identifier.setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c878281");

    observation.setStatus(Observation.ObservationStatus.FINAL);

    CodeableConcept obsCode = new CodeableConcept();

    observation.setCode(obsCode);

    Quantity quantity = new Quantity();
    quantity.setValue(new java.math.BigDecimal("123.45"));
    quantity.setUnit("mm[Hg]");
    quantity.setSystem("http://unitsofmeasure.org");
    observation.setValue(quantity);

    ObservationComponentComponent component = observation.addComponent();

    CodeableConcept code = new CodeableConcept()
        .addCoding(new Coding()
            .setCode("abc")
            .setSystem("PLACEHOLDER"));

    component.setCode(code);

    return observation;
  }

  /**
   * Returns a new Patient for testing.
   *
   * @return a FHIR Patient for testing.
   */
  public static Patient newPatient() {

    Patient patient = new Patient();

    patient.setId("test-patient");
    patient.setGender(AdministrativeGender.MALE);
    patient.setActive(true);
    patient.setMultipleBirth(new IntegerType(1));

    patient.setBirthDateElement(new DateType("1945-01-02"));

    patient.addGeneralPractitioner().setReference("Practitioner/12345");

    Identifier practitionerIdentifier = new Identifier();
    practitionerIdentifier.setId("P123456");
    practitionerIdentifier.getAssigner().setReference("Organization/123456");
    patient.getGeneralPractitionerFirstRep().setIdentifier(practitionerIdentifier);

    Address address = patient.addAddress();
    address.addLine("123 Fake Street");
    address.setCity("Chicago");
    address.setState("IL");
    address.setDistrict("12345");

    Extension birthSex = patient.addExtension();

    birthSex.setUrl(US_CORE_BIRTHSEX);
    birthSex.setValue(new CodeType("M"));

    Extension ethnicity = patient.addExtension();
    ethnicity.setUrl(US_CORE_ETHNICITY);
    ethnicity.setValue(null);

    Coding ombCoding = new Coding();

    ombCoding.setSystem("urn:oid:2.16.840.1.113883.6.238");
    ombCoding.setCode("2135-2");
    ombCoding.setDisplay("Hispanic or Latino");

    // Add category to ethnicity extension
    Extension ombCategory = ethnicity.addExtension();

    ombCategory.setUrl("ombCategory");
    ombCategory.setValue(ombCoding);

    // Add multiple detailed sub-extension to ethnicity extension
    Coding detailedCoding1 = new Coding();
    detailedCoding1.setSystem("urn:oid:2.16.840.1.113883.6.238");
    detailedCoding1.setCode("2165-9");
    detailedCoding1.setDisplay("South American");

    Coding detailedCoding2 = new Coding();
    detailedCoding2.setSystem("urn:oid:2.16.840.1.113883.6.238");
    detailedCoding2.setCode("2166-7");
    detailedCoding2.setDisplay("Argentinean");

    final Extension detailed1 = ethnicity.addExtension();
    detailed1.setUrl("detailed");
    detailed1.setValue(detailedCoding1);

    final Extension detailed2 = ethnicity.addExtension();
    detailed2.setUrl("detailed");
    detailed2.setValue(detailedCoding2);

    // Add text display to ethnicity extension
    Extension ethnicityText = ethnicity.addExtension();
    ethnicityText.setUrl("text");
    ethnicityText.setValue(new StringType("Not Hispanic or Latino"));

    // Human Name
    HumanName humanName = new HumanName();
    humanName.setFamily("family_name");
    humanName.addGiven("given_name");
    humanName.addGiven("middle_name");
    patient.addName(humanName);


    Meta meta = new Meta();

    List<Coding> tag = meta.getTag();

    Coding code = new Coding();
    code.setCode("test-code");
    code.setSystem("test-system");

    tag.add(code);
    meta.setTag(tag);
    patient.setMeta(meta);

    return patient;
  }

  /**
   * Returns a new Medication for testing.
   *
   * @return a FHIR Medication for testing.
   */
  public static Medication newMedication(String id) {

    Medication medication = new Medication();

    medication.setId(id);

    CodeableConcept itemCodeableConcept = new CodeableConcept();
    itemCodeableConcept.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("103109")
        .setDisplay("Vitamin E 3 MG Oral Tablet [Ephynal]")
        .setUserSelected(true);

    MedicationIngredientComponent ingredientComponent = new MedicationIngredientComponent();
    ingredientComponent.setItem(itemCodeableConcept);

    medication.addIngredient(ingredientComponent);

    Reference itemReference = new Reference("test-item-reference");

    /* TODO update for R4
    MedicationPackageContentComponent medicationPackageContentComponent =
        new MedicationPackageContentComponent();
    medicationPackageContentComponent.setItem(itemReference);

    MedicationPackageComponent medicationPackageComponent = new MedicationPackageComponent();
    medicationPackageComponent.addContent(medicationPackageContentComponent);

    medication.setPackage(medicationPackageComponent);
     */

    return medication;
  }

  /**
   * Returns a new MedicationRequest for testing.
   *
   * @return a FHIR MedicationRequest for testing.
   */
  public static MedicationRequest newMedicationRequest() {

    MedicationRequest medicationRequest = new MedicationRequest();

    medicationRequest.setId("test-medication-request");

    CodeableConcept itemCodeableConcept = new CodeableConcept();
    itemCodeableConcept.addCoding()
        .setSystem("http://www.nlm.nih.gov/research/umls/rxnorm")
        .setCode("103109")
        .setDisplay("Vitamin E 3 MG Oral Tablet [Ephynal]")
        .setUserSelected(true);

    medicationRequest.setMedication(itemCodeableConcept);

    medicationRequest
        .setSubject(new Reference("Patient/12345").setDisplay("Here is a display for you."));

    medicationRequest.setDosageInstruction(ImmutableList.of(
        new Dosage().setTiming(new Timing().setRepeat(new TimingRepeatComponent().setCount(10)))));

    medicationRequest.setSubstitution(
        new MedicationRequestSubstitutionComponent().setAllowed(new BooleanType(true)));

    return medicationRequest;
  }

  /**
   * Returns a new Provenance for testing.
   *
   * @return a FHIR Provenance for testing.
   */
  public static Provenance newProvenance() {

    Provenance provenance = new Provenance();

    provenance.setId("test-provenance");

    return provenance;
  }

  /**
   * Returns a new Patient from Bunsen Test profile for testing.
   *
   * @return a FHIR Patient for testing.
   */
  public static Patient newBunsenTestProfilePatient() {
    Patient patient = new Patient();

    patient.setId("test-bunsen-test-profile-patient");
    patient.setGender(AdministrativeGender.MALE);
    patient.setActive(true);
    patient.setMultipleBirth(new IntegerType(1));

    patient.setBirthDateElement(new DateType("1945-01-01"));

    // set extension field
    Extension booleanField = patient.addExtension();
    booleanField.setUrl(BUNSEN_TEST_BOOLEAN_FIELD);
    booleanField.setValue(new BooleanType(true));

    Extension integerField = patient.addExtension();
    integerField.setUrl(BUNSEN_TEST_INTEGER_FIELD);
    integerField.setValue(new IntegerType(45678));

    Extension integerArrayField1 = patient.addExtension();
    integerArrayField1.setUrl(BUNSEN_TEST_INTEGER_ARRAY_FIELD);
    integerArrayField1.setValue(new IntegerType(6666));

    Extension integerArrayField2 = patient.addExtension();
    integerArrayField2.setUrl(BUNSEN_TEST_INTEGER_ARRAY_FIELD);
    integerArrayField2.setValue(new IntegerType(9999));

    // add multiple nested extensions
    final Extension nestedExtension1 = patient.addExtension();
    nestedExtension1.setUrl(BUNSEN_TEST_NESTED_EXT_FIELD);
    nestedExtension1.setValue(null);

    final Extension nestedExtension2 = patient.addExtension();
    nestedExtension2.setUrl(BUNSEN_TEST_NESTED_EXT_FIELD);
    nestedExtension2.setValue(null);

    // add text as sub-extension to nestedExtension
    final Extension textExt1 = nestedExtension1.addExtension();
    textExt1.setUrl("text");
    textExt1.setValue(new StringType("Text1 Sub-extension of nestedExtension1 field"));

    final Extension textExt2 = nestedExtension1.addExtension();
    textExt2.setUrl("text");
    textExt2.setValue(new StringType("Text2 Sub-extension of nestedExtension1 field"));

    final Extension textExt3 = nestedExtension2.addExtension();
    textExt3.setUrl("text");
    textExt3.setValue(new StringType("Text3 Sub-extension of nestedExtension2 field"));

    // add multiple codeableConcept extensions to nestedExtension
    final CodeableConcept codeableconcept1 = new CodeableConcept();
    codeableconcept1.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("CC1")
        .setDisplay("CC1 - Codeable Concept Extension")
        .setUserSelected(true);

    final CodeableConcept codeableconcept2 = new CodeableConcept();
    codeableconcept2.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("CC2")
        .setDisplay("CC2 - Codeable Concept Extension")
        .setUserSelected(true);

    final CodeableConcept codeableconcept3 = new CodeableConcept();
    codeableconcept3.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("CC3")
        .setDisplay("CC3 - Codeable Concept Extension")
        .setUserSelected(true);

    final Extension codeableConceptExt1 = nestedExtension1.addExtension();
    codeableConceptExt1.setUrl(BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD);
    codeableConceptExt1.setValue(codeableconcept1);

    final Extension codeableConceptExt2 = nestedExtension1.addExtension();
    codeableConceptExt2.setUrl(BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD);
    codeableConceptExt2.setValue(codeableconcept2);

    final Extension codeableConceptExt3 = nestedExtension2.addExtension();
    codeableConceptExt3.setUrl(BUNSEN_TEST_CODEABLE_CONCEPT_EXT_FIELD);
    codeableConceptExt3.setValue(codeableconcept3);

    // add multiple ModifierExtension fields
    Extension stringModifierExtension = patient.addModifierExtension();
    stringModifierExtension.setUrl(BUNSEN_TEST_STRING_MODIFIER_EXT_FIELD);
    stringModifierExtension.setValue(new StringType("test string modifier value"));

    CodeableConcept concept1 = new CodeableConcept();
    concept1.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("C-1")
        .setDisplay("C-1 Codeable Concept Modifier Extension")
        .setUserSelected(true);

    Extension codeableConceptField = patient.addModifierExtension();
    codeableConceptField.setUrl(BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD);
    codeableConceptField.setValue(concept1);

    CodeableConcept concept2 = new CodeableConcept();
    concept2.addCoding()
        .setSystem("http://snomed.info/sct")
        .setCode("C-2")
        .setDisplay("C-2 Codeable Concept Modifier Extension")
        .setUserSelected(true);

    Extension codeableConceptField2 = patient.addModifierExtension();
    codeableConceptField2.setUrl(BUNSEN_TEST_CODEABLE_CONCEPT_MODIFIER_EXT_FIELD);
    codeableConceptField2.setValue(concept2);

    return patient;
  }
}
