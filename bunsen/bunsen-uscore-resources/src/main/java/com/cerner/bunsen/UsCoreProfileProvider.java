package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.profiles.BaseProfileProvider;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SPI implementation to load profile resources for US Core. */
public class UsCoreProfileProvider extends BaseProfileProvider {
  private static final Logger log = LoggerFactory.getLogger(UsCoreProfileProvider.class);

  private static void load(
      FhirContext context,
      PrePopulatedValidationSupport support,
      IParser jsonParser,
      String resource,
      Map<String, String> profileMapping) {

    try (InputStream input =
        UsCoreProfileProvider.class.getClassLoader().getResourceAsStream(resource)) {
      IBaseResource definition = jsonParser.parseResource(new InputStreamReader(input));
      support.addStructureDefinition(definition);
      mapResourceToProfile(context, definition, profileMapping);
    } catch (IOException exception) {
      throw new RuntimeException("Failed to load US Core resource " + resource, exception);
    }
  }

  private static Map<String, String> addUsCoreDefinitionsForDstu3(
      PrePopulatedValidationSupport support, FhirContext context) {
    Map<String, String> mapping = new HashMap<>();
    IParser parser = context.newJsonParser();
    // TODO remove non-StructureDefinition files in definitions-stu3/ if they are not needed.
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-allergyintolerance.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-birthsex.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-careplan.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-careteam.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-condition.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-device.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-diagnosticreport.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-direct.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-documentreference.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-encounter.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-ethnicity.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-goal.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-immunization.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-location.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-medication.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-medicationrequest.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-medicationstatement.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-observationresults.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-organization.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-patient.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-practitioner.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-practitionerrole.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-procedure.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-profile-link.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-race.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-stu3/StructureDefinition-us-core-smokingstatus.json",
        mapping);
    return mapping;
  }

  private static Map<String, String> addUsCoreDefinitionsForR4(
      PrePopulatedValidationSupport support, FhirContext context) {
    Map<String, String> mapping = new HashMap<>();
    IParser parser = context.newJsonParser();
    load(
        context,
        support,
        parser,
        "definitions-r4/"
            + "StructureDefinition-head-occipital-frontal-circumference-percentile.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-pediatric-bmi-for-age.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-pediatric-weight-for-height.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-allergyintolerance.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-birthsex.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-blood-pressure.json",
        mapping);
    load(context, support, parser, "definitions-r4/StructureDefinition-us-core-bmi.json", mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-body-height.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-body-temperature.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-body-weight.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-careplan.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-careteam.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-condition-encounter-diagnosis.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-condition-problems-health-concerns.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-diagnosticreport-lab.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-diagnosticreport-note.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-direct.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-documentreference.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-encounter.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-ethnicity.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-extension-questionnaire-uri.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-genderIdentity.json",
        mapping);
    load(context, support, parser, "definitions-r4/StructureDefinition-us-core-goal.json", mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-head-circumference.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-heart-rate.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-immunization.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-implantable-device.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-location.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-medication.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-medicationrequest.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-clinical-test.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-imaging.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-lab.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-sdoh-assessment.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-sexual-orientation.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-survey.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-organization.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-patient.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-practitioner.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-practitionerrole.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-procedure.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-provenance.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-pulse-oximetry.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-questionnaireresponse.json",
        mapping);
    load(context, support, parser, "definitions-r4/StructureDefinition-us-core-race.json", mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-relatedperson.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-respiratory-rate.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-servicerequest.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-smokingstatus.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-vital-signs.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions-r4/StructureDefinition-us-core-observation-social-history.json",
        mapping);
    return mapping;
  }

  private static Map<String, String> addUsCoreDefinitions(
      PrePopulatedValidationSupport support, FhirContext context) {
    Preconditions.checkArgument(
        context.getVersion().getVersion() == FhirVersionEnum.DSTU3
            || context.getVersion().getVersion() == FhirVersionEnum.R4);

    if (context.getVersion().getVersion() == FhirVersionEnum.DSTU3) {
      return addUsCoreDefinitionsForDstu3(support, context);
    }

    if (context.getVersion().getVersion() == FhirVersionEnum.R4) {
      return addUsCoreDefinitionsForR4(support, context);
    }
    return new HashMap<>();
  }

  @Override
  public Map<String, String> loadStructureDefinitions(
      FhirContext context, List<String> structureDefinitionsPath) {
    throw new IllegalArgumentException("This method is not supported");
  }

  @Override
  public Map<String, String> loadStructureDefinitions(FhirContext context) {
    support = new PrePopulatedValidationSupport(context);

    Map<String, String> baseProfileMappings = super.loadStructureDefinitions(context);
    Map<String, String> usCoreProfileMappings = addUsCoreDefinitions(support, context);

    context.setValidationSupport(support);
    return mergeProfileMappings(baseProfileMappings, usCoreProfileMappings);
  }
}
