package com.cerner.bunsen.test.profiles.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.IValidationSupport;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.profiles.ProfileProvider;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.dstu3.model.StructureDefinition;

/**
 * {@link ProfileProvider} implementation to load resources required for Bunsen Testing.
 */
public class BunsenTestStu3ProfileProvider implements ProfileProvider {

  private static void load(PrePopulatedValidationSupport support, IParser jsonParser,
      String resource) {

    try (InputStream input = BunsenTestStu3ProfileProvider.class.getClassLoader()
        .getResourceAsStream(resource)) {

      StructureDefinition definition = (StructureDefinition) jsonParser
          .parseResource(new InputStreamReader(input));

      support.addStructureDefinition(definition);

    } catch (IOException exception) {

      throw new RuntimeException(exception);
    }
  }

  private static void addBunsenTestStu3ProfileDefinitions(PrePopulatedValidationSupport support,
      FhirContext context) {

    IParser parser = context.newJsonParser();

    load(support, parser, "definitions/StructureDefinition-bunsen-test-profile-booleanfield.json");
    load(support, parser, "definitions/StructureDefinition-bunsen-test-profile-integerfield.json");
    load(support, parser, "definitions/StructureDefinition-bunsen-test-profile-Patient.json");
    load(support, parser,
        "definitions/StructureDefinition-bunsen-test-profile-integerArrayField.json");
    load(support, parser,
        "definitions/StructureDefinition-bunsen-test-profile-nested-extension.json");
    load(support, parser,
        "definitions/StructureDefinition-bunsen-test-profile-codeableconcept-ext.json");
    load(support, parser,
        "definitions/StructureDefinition-bunsen-test-codeableConcept-modifierExt.json");
    load(support, parser,
        "definitions/StructureDefinition-bunsen-test-string-modifierExt.json");
  }

  @Override
  public void loadStructureDefinitions(FhirContext context) {

    IValidationSupport defaultSupport = context.getValidationSupport();

    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport(context);

    List<StructureDefinition> defaultDefinitions = defaultSupport.fetchAllStructureDefinitions();

    for (StructureDefinition definition : defaultDefinitions) {

      support.addStructureDefinition(definition);

    }

    addBunsenTestStu3ProfileDefinitions(support, context);

    context.setValidationSupport(support);
  }
}
