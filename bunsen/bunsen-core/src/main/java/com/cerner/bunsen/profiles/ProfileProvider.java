package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;

/** SPI interface to load FHIR structure definitions. */
public interface ProfileProvider {

  /**
   * Adds any profiles that this provider has for the given `context`. It is the implementation's
   * responsibility to check the `context` version and only load profiles that matches it.
   *
   * @param context The context to which the profiles are added.
   */
  void loadStructureDefinitions(FhirContext context);
}
