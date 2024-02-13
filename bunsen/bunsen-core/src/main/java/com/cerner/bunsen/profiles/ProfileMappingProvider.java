package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;
import java.util.Map;
import javax.annotation.Nullable;

/** The interface defining the APIs to load the structure definitions into the FhirContext */
public interface ProfileMappingProvider {
  /**
   * Adds any profiles that this provider has for the given `context`.
   *
   * @param context The context to which the profiles are added.
   * @param structureDefinitionPath the path containing the list of structure definitions to be used
   * @return the map containing the resource to profile mapping.
   */
  Map<String, String> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionPath);
}
