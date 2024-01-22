package com.cerner.bunsen.test.profiles.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.profiles.BaseProfileProvider;
import com.cerner.bunsen.profiles.ProfileProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.dstu3.model.StructureDefinition;

// TODO make this generic for R4: https://github.com/google/fhir-data-pipes/issues/558
/** {@link ProfileProvider} implementation to load resources required for Bunsen Testing. */
public class BunsenTestStu3ProfileProvider extends BaseProfileProvider {

  private static void load(
      FhirContext context,
      PrePopulatedValidationSupport support,
      IParser jsonParser,
      String resource,
      Map<String, String> profileMapping) {

    try (InputStream input =
        BunsenTestStu3ProfileProvider.class.getClassLoader().getResourceAsStream(resource)) {

      StructureDefinition definition =
          (StructureDefinition) jsonParser.parseResource(new InputStreamReader(input));

      mapResourceToProfile(context, definition, profileMapping);
      support.addStructureDefinition(definition);

    } catch (IOException exception) {

      throw new RuntimeException(exception);
    }
  }

  private static Map<String, String> addBunsenTestStu3ProfileDefinitions(
      PrePopulatedValidationSupport support, FhirContext context) {
    Map<String, String> mapping = new HashMap<>();
    IParser parser = context.newJsonParser();

    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-booleanfield.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-integerfield.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-Patient.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-integerArrayField.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-nested-extension.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-profile-codeableconcept-ext.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-codeableConcept-modifierExt.json",
        mapping);
    load(
        context,
        support,
        parser,
        "definitions/StructureDefinition-bunsen-test-string-modifierExt.json",
        mapping);
    return mapping;
  }

  @Override
  public Map<String, String> loadStructureDefinitions(FhirContext context) {

    Map<String, String> resourceProfileMap = new HashMap<>();
    if (context.getVersion().getVersion() != FhirVersionEnum.DSTU3) {
      // The context is for a different FHIR version so we should not load the structure-defs.
      return resourceProfileMap;
    }

    support = new PrePopulatedValidationSupport(context);
    Map<String, String> baseProfileMappings = super.loadStructureDefinitions(context);
    Map<String, String> usCoreProfileMappings =
        addBunsenTestStu3ProfileDefinitions(support, context);

    context.setValidationSupport(support);
    return mergeProfileMappings(baseProfileMappings, usCoreProfileMappings);
  }

  @Override
  public Map<String, String> loadStructureDefinitions(
      FhirContext context, List<String> customDefinitionsPath) {
    throw new IllegalArgumentException("This method is not yet supported");
  }
}
