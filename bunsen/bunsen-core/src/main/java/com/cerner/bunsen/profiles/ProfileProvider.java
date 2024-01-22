package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Map;

/** SPI interface to load FHIR structure definitions. */
public interface ProfileProvider {

  /**
   * Adds any profiles that this provider has for the given `context`. It is the implementation's
   * responsibility to check the `context` version and only load profiles that matches it. This also
   * returns a map containing the resource type and fhir profile that has been loaded against this
   * resource type.
   *
   * <p>Currently only one profile is supported for any resource type.
   *
   * @param context The context to which the profiles are added.
   * @return the map containing the resource type and the fhir profile that has been mapped
   */
  Map<String, String> loadStructureDefinitions(FhirContext context);

  /**
   * Adds any profiles that this provider has for the given `context`. The `customDefinitionsPath`
   * also defines the list of paths containing the structure definitions to be loaded for any
   * profiles
   *
   * @param context The context to which the profiles are added.
   * @param customDefinitionsPath the list of paths containing the structure definitions for the
   *     profiles
   * @return the map containing the resource type and the fhir profile that has been mapped
   */
  Map<String, String> loadStructureDefinitions(
      FhirContext context, List<String> customDefinitionsPath);
}
