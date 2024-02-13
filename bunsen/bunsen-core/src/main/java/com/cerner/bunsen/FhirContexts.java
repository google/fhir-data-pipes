package com.cerner.bunsen;

import static ca.uhn.fhir.context.FhirVersionEnum.DSTU3;
import static ca.uhn.fhir.context.FhirVersionEnum.R4;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.profiles.ProfileProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Loader for FHIR contexts. Unlike the provided {@link FhirContext} loader, this implementation
 * caches the contexts for reuse, and also loads profiles that implement the {@link ProfileProvider}
 * SPI.
 */
public class FhirContexts {

  /** Cache of FHIR contexts. */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new HashMap();

  /** Loads structure definitions for supported profiles. */
  private static void loadProfiles(FhirContext context) {

    ServiceLoader<ProfileProvider> loader = ServiceLoader.load(ProfileProvider.class);

    loader.forEach(provider -> provider.loadStructureDefinitions(context));
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirContext contextFor(FhirVersionEnum fhirVersion) {

    synchronized (FHIR_CONTEXTS) {
      FhirContext context = FHIR_CONTEXTS.get(fhirVersion);

      if (context == null) {

        context = new FhirContext(fhirVersion);

        loadProfiles(context);

        FHIR_CONTEXTS.put(fhirVersion, context);
      }

      return context;
    }
  }

  /**
   * Returns a builder to create encoders for FHIR STU3.
   *
   * @return a builder for encoders.
   */
  public static FhirContext forStu3() {

    return contextFor(DSTU3);
  }

  /**
   * Returns a builder to create encoders for FHIR R4.
   *
   * @return a builder for encoders.
   */
  public static FhirContext forR4() {

    return contextFor(R4);
  }
}
