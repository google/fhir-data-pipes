package com.cerner.bunsen.common;

import java.util.Arrays;
import java.util.List;

public class R4UsCoreProfileData {

  public static final List<String> US_CORE_PATIENT_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Patient",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient");

  public static final List<String> US_CORE_OBSERVATION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Observation",
          "http://hl7.org/fhir/us/core/StructureDefinition/head-occipital-frontal-circumference-percentile",
          "http://hl7.org/fhir/us/core/StructureDefinition/pediatric-bmi-for-age",
          "http://hl7.org/fhir/us/core/StructureDefinition/pediatric-weight-for-height",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-blood-pressure",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-bmi",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-body-height",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-body-temperature",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-body-weight",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-head-circumference",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-heart-rate",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-clinical-test",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-imaging",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-sdoh-assessment",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-sexual-orientation",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-social-history",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-survey",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-pulse-oximetry",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-respiratory-rate",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-smokingstatus",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-vital-signs");

  public static final List<String> US_CORE_CONDITION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Condition",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition-encounter-diagnosis",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition-problems-health-concerns");

  public static final List<String> US_CORE_ENCOUNTER_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Encounter",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter");

  public static final List<String> US_CORE_MEDICATION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Medication",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medication");

  public static final List<String> US_CORE_QUESTIONNAIRE_RESPONSE_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/QuestionnaireResponse",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-questionnaireresponse");
}
