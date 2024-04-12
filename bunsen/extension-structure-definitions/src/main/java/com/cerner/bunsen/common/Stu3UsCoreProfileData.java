package com.cerner.bunsen.common;

import java.util.Arrays;
import java.util.List;

public class Stu3UsCoreProfileData {

  public static final List<String> US_CORE_PATIENT_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Patient",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient");

  public static final List<String> US_CORE_OBSERVATION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Observation",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observationresults",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-smokingstatus");

  public static final List<String> US_CORE_CONDITION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Condition",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition");

  public static final List<String> US_CORE_MEDICATION_PROFILES =
      Arrays.asList(
          "http://hl7.org/fhir/StructureDefinition/Medication",
          "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medication");
}
