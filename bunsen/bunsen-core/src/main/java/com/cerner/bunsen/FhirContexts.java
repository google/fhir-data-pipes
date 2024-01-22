package com.cerner.bunsen;

import static ca.uhn.fhir.context.FhirVersionEnum.DSTU3;
import static ca.uhn.fhir.context.FhirVersionEnum.R4;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.profiles.ProfileProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

/**
 * Loader for FHIR contexts. Unlike the provided {@link FhirContext} loader, this implementation
 * caches the contexts for reuse, and also loads profiles that implement the {@link ProfileProvider}
 * SPI.
 */
public class FhirContexts {

  /** Cache of FHIR contexts. */
  private static final Map<FhirVersionEnum, FhirContext> FHIR_CONTEXTS = new HashMap();

  /**
   * Mapping of resource types to the FHIR profiles that needs to be applied on the FHIR resources
   */
  private static final Map<FhirVersionEnum, Map<String, String>> RESOURCE_PROFILE_MAPPINGS =
      new HashMap<>();

  /** Loads structure definitions for supported profiles. */
  private static void loadProfiles(FhirContext context) {
    ServiceLoader<ProfileProvider> loader = ServiceLoader.load(ProfileProvider.class);
    loader.forEach(
        provider -> {
          Map<String, String> resourceProfileMappings = provider.loadStructureDefinitions(context);
          updateResourceProfileMappings(context.getVersion().getVersion(), resourceProfileMappings);
        });
  }

  /**
   * Loads the custom structure definitions present in the given {@code structureDefinitionPaths}
   * paths.
   *
   * @param context the context into which the structure definitions are loaded
   * @param structureDefinitionPaths the list of paths containing the structure definition files
   */
  private static void loadProfiles(FhirContext context, List<String> structureDefinitionPaths) {
    ServiceLoader<ProfileProvider> loader = ServiceLoader.load(ProfileProvider.class);
    loader.forEach(
        provider -> {
          Map<String, String> resourceProfileMappings =
              provider.loadStructureDefinitions(context, structureDefinitionPaths);
          updateResourceProfileMappings(context.getVersion().getVersion(), resourceProfileMappings);
        });
  }

  private static void updateResourceProfileMappings(
      FhirVersionEnum fhirVersionEnum, Map<String, String> resourceProfileMappings) {
    if (resourceProfileMappings != null && !resourceProfileMappings.isEmpty()) {
      Map<String, String> globalResourceMapping =
          RESOURCE_PROFILE_MAPPINGS.computeIfAbsent(fhirVersionEnum, key -> new HashMap<>());
      for (Entry<String, String> mapping : resourceProfileMappings.entrySet()) {
        globalResourceMapping.put(mapping.getKey(), mapping.getValue());
      }
    }
  }

  /** Retrieves the resource profile mappings for the given fhirVersion */
  public static Map<String, String> resourceProfileMappingsFor(FhirVersionEnum fhirVersion) {
    return RESOURCE_PROFILE_MAPPINGS.get(fhirVersion);
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
   * Similar to {@link #contextFor(FhirVersionEnum fhirVersion)}, but additionally loads the
   * structure definitions from the list of the paths defined by structureDefinitionPaths.
   *
   * @param fhirVersion the version of FHIR to use
   * @param structureDefinitionPaths the list of paths containing structure definition files to load
   * @return the FhirContext
   */
  public static FhirContext contextFor(
      FhirVersionEnum fhirVersion, List<String> structureDefinitionPaths) {

    synchronized (FHIR_CONTEXTS) {
      FhirContext context = FHIR_CONTEXTS.get(fhirVersion);

      if (context == null) {

        context = new FhirContext(fhirVersion);

        loadProfiles(context, structureDefinitionPaths);

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
   * Similar to {@link #forStu3()}, the {@code structureDefinitionsPath} is the list of paths
   * containing the structure definition files to be loaded into the FHIRContext.
   *
   * @param structureDefinitionsPath the list of paths containing the structure definition files
   * @return a builder for encoders.
   */
  public static FhirContext forStu3(List<String> structureDefinitionsPath) {

    return contextFor(DSTU3, structureDefinitionsPath);
  }

  /**
   * Returns a builder to create encoders for FHIR R4.
   *
   * @return a builder for encoders.
   */
  public static FhirContext forR4() {

    return contextFor(R4);
  }

  /**
   * Similar to {@link #forR4()}}, the {@code structureDefinitionsPath} is the list of paths
   * containing the structure definition files to be loaded into the FHIRContext.
   *
   * @param structureDefinitionsPath the list of paths containing the structure definition files
   * @return a builder for encoders.
   */
  public static FhirContext forR4(List<String> structureDefinitionsPath) {

    return contextFor(R4, structureDefinitionsPath);
  }

  /**
   * Removes all the FhirContext and the corresponding mappings for the given fhirVersionEnum.
   *
   * @param fhirVersionEnum the fhir version for which the details are to be removed
   */
  public static void deRegisterFhirContext(FhirVersionEnum fhirVersionEnum) {
    synchronized (FHIR_CONTEXTS) {
      FHIR_CONTEXTS.remove(fhirVersionEnum);
      RESOURCE_PROFILE_MAPPINGS.remove(fhirVersionEnum);
    }
  }
}
