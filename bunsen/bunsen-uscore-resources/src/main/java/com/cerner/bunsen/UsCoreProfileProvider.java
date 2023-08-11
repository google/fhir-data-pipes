package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.support.IValidationSupport;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.profiles.ProfileProvider;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SPI implementation to load profile resources for US Core. */
public class UsCoreProfileProvider implements ProfileProvider {
  private static final Logger log = LoggerFactory.getLogger(UsCoreProfileProvider.class);

  private static void load(
      PrePopulatedValidationSupport support, IParser jsonParser, String resource) {

    try (InputStream input =
        UsCoreProfileProvider.class.getClassLoader().getResourceAsStream(resource)) {
      IBaseResource definition = jsonParser.parseResource(new InputStreamReader(input));
      support.addStructureDefinition(definition);
    } catch (IOException exception) {
      throw new RuntimeException("Failed to load US Core resource " + resource, exception);
    }
  }

  private static void addUsCoreDefinitionsForDstu3(
      PrePopulatedValidationSupport support, FhirContext context) {
    IParser parser = context.newJsonParser();
    // TODO remove non-StructureDefinition files in definitions-stu3/ if they are not needed.
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-allergyintolerance.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-birthsex.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-careplan.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-careteam.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-condition.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-device.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-diagnosticreport.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-direct.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-documentreference.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-encounter.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-ethnicity.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-goal.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-immunization.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-location.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-medication.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-medicationrequest.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-medicationstatement.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-observationresults.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-organization.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-patient.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-practitioner.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-practitionerrole.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-procedure.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-profile-link.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-race.json");
    load(support, parser, "definitions-stu3/StructureDefinition-us-core-smokingstatus.json");
  }

  private static void addUsCoreDefinitionsForR4(
      PrePopulatedValidationSupport support, FhirContext context) {
    IParser parser = context.newJsonParser();
    load(
        support,
        parser,
        "definitions-r4/"
            + "StructureDefinition-head-occipital-frontal-circumference-percentile.json");
    load(support, parser, "definitions-r4/StructureDefinition-pediatric-bmi-for-age.json");
    load(support, parser, "definitions-r4/StructureDefinition-pediatric-weight-for-height.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-allergyintolerance.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-birthsex.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-blood-pressure.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-bmi.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-body-height.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-body-temperature.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-body-weight.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-careplan.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-careteam.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-condition-encounter-diagnosis.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-condition-problems-health-concerns.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-diagnosticreport-lab.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-diagnosticreport-note.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-direct.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-documentreference.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-encounter.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-ethnicity.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-extension-questionnaire-uri.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-genderIdentity.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-goal.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-head-circumference.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-heart-rate.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-immunization.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-implantable-device.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-location.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-medication.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-medicationrequest.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-clinical-test.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-observation-imaging.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-observation-lab.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-sdoh-assessment.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-sexual-orientation.json");
    load(
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-social-history.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-observation-survey.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-organization.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-patient.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-practitioner.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-practitionerrole.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-procedure.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-provenance.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-pulse-oximetry.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-questionnaireresponse.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-race.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-relatedperson.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-respiratory-rate.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-servicerequest.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-smokingstatus.json");
    load(support, parser, "definitions-r4/StructureDefinition-us-core-vital-signs.json");
  }

  private static void addUsCoreDefinitions(
      PrePopulatedValidationSupport support, FhirContext context) {
    Preconditions.checkArgument(
        context.getVersion().getVersion() == FhirVersionEnum.DSTU3
            || context.getVersion().getVersion() == FhirVersionEnum.R4);

    if (context.getVersion().getVersion() == FhirVersionEnum.DSTU3) {
      addUsCoreDefinitionsForDstu3(support, context);
    }

    if (context.getVersion().getVersion() == FhirVersionEnum.R4) {
      addUsCoreDefinitionsForR4(support, context);
    }
  }

  @Override
  public void loadStructureDefinitions(FhirContext context) {

    if (context.getVersion().getVersion() != FhirVersionEnum.DSTU3
        && context.getVersion().getVersion() != FhirVersionEnum.R4) {
      // We don't have the US-core definitions for this version of FHIR.
      log.warn(
          "Skipped loading US-core profiles for FhirContext version {} as it is not {} or {} ",
          context.getVersion().getVersion(),
          FhirVersionEnum.DSTU3,
          FhirVersionEnum.R4);
      return;
    }

    IValidationSupport defaultSupport = context.getValidationSupport();
    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport(context);
    List<IBaseResource> defaultDefinitions = defaultSupport.fetchAllStructureDefinitions();
    for (IBaseResource definition : defaultDefinitions) {
      support.addStructureDefinition(definition);
    }

    addUsCoreDefinitions(support, context);

    context.setValidationSupport(support);
  }
}
