package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;

/**
 * SPI interface to load FHIR structure definitions.
 */
public interface ProfileProvider {

  void loadStructureDefinitions(FhirContext context);

}
