package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class loads the profiles for the Fhir contexts and caches them for reuse. Profiles are
 * loaded based on the implementation of {@link ProfileMappingProvider}.
 */
public class ProfileMapperFhirContexts {

  private static final Logger logger =
      LoggerFactory.getLogger(ProfileMapperFhirContexts.class.getName());
  private final Map<FhirVersionEnum, FhirContextData> fhirContextMappings;

  @SuppressWarnings(
      "NullAway.Init") // This is initialized/used via the singleton pattern getInstance method.
  private static ProfileMapperFhirContexts instance;

  @SuppressWarnings("NullAway") // This is initialized in the private constructor.
  private final ProfileMappingProvider profileMappingProvider;

  private ProfileMapperFhirContexts() {
    this.fhirContextMappings = Maps.newHashMap();
    this.profileMappingProvider = new ProfileMappingProvider();
  }

  /** This method returns a singleton instance of the current class */
  public static ProfileMapperFhirContexts getInstance() {
    synchronized (ProfileMapperFhirContexts.class) {
      if (instance == null) {
        instance = new ProfileMapperFhirContexts();
      }
      return instance;
    }
  }

  /**
   * Returns the FHIR context for the given version and the structureDefinitionsPath. For the given
   * fhirVersion, this method loads all the base profiles and also the custom profiles defined in
   * the structureDefinitionsPath. This method caches the Fhir context for the given input
   * combination of fhirVersion and structureDefinitionsPath, and returns the same FhirContext for
   * any further invocations of similar arguments. It returns an error if the method is called with
   * a different structureDefinitionsPath for the same fhirVersion earlier.
   *
   * @param fhirVersion the version of FHIR to use
   * @param structureDefinitionsPath The path containing the custom structure definitions; if it
   *     starts with `classpath:` then the StructureDefinitions in classpath are used instead.
   * @return the FhirContext
   * @throws ProfileException if there are any errors while loading and mapping the structure
   *     definitions
   */
  public synchronized FhirContext contextFor(
      FhirVersionEnum fhirVersion, @Nullable String structureDefinitionsPath)
      throws ProfileException {
    structureDefinitionsPath = Strings.nullToEmpty(structureDefinitionsPath);
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData != null) {
      isContextLoadedWithDifferentConfig(fhirContextData, structureDefinitionsPath);
    } else {
      // We are creating a new FhirContext instance here using the constructor method instead of the
      // FhirContext.forCached(), because we need to load different Structure Definitions during
      // unit tests for the same version, using static cached context limits this.
      FhirContext context = new FhirContext(fhirVersion);
      Map<String, List<String>> profileMap =
          loadStructureDefinitions(context, structureDefinitionsPath);
      fhirContextData = new FhirContextData(context, structureDefinitionsPath, profileMap);
      fhirContextMappings.put(fhirVersion, fhirContextData);
    }
    return fhirContextData.fhirContext;
  }

  private void isContextLoadedWithDifferentConfig(
      FhirContextData fhirContextData, String structureDefinitionsPath) throws ProfileException {
    if (!Objects.equals(fhirContextData.structureDefinitionsPath, structureDefinitionsPath)) {
      String errorMsg =
          String.format(
              "Failed to initialise FhirContext with structureDefinitionsPath=%s, it is"
                  + " already initialised with different structureDefinitionsPath=%s",
              structureDefinitionsPath, fhirContextData.structureDefinitionsPath);
      logger.error(errorMsg);
      throw new ProfileException(errorMsg);
    }
  }

  /**
   * Loads base structure definitions and also the custom structure definitions present in the given
   * {@code structureDefinitionsPath} path into the context.
   *
   * @param context the context into which the structure definitions are loaded
   * @param structureDefinitionsPath the path containing the custom structure definitions
   * @return the map containing the resource to profile urls mappings
   * @throws ProfileException if there are any errors while loading and mapping the structure
   *     definitions
   */
  private Map<String, List<String>> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionsPath) throws ProfileException {
    return profileMappingProvider.loadStructureDefinitions(context, structureDefinitionsPath);
  }

  /**
   * Returns the list of mapped profile urls for the given resourceType.
   *
   * @param fhirVersion the fhir version for which the mapping needs to be returned
   * @param resourceType the resource type
   * @return the list of profile urls
   * @throws ProfileException if the FhirContext is not initialised for the given fhirVersion
   */
  @Nullable
  public List<String> getMappedProfilesForResource(FhirVersionEnum fhirVersion, String resourceType)
      throws ProfileException {
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      String errorMsg =
          String.format(
              "The fhirContext is not initialised yet for version=%s, Please initialise the"
                  + " fhirContext using one of the flavors of contextFor() methods before fetching"
                  + " the mapped profile.",
              fhirVersion);
      logger.error(errorMsg);
      throw new ProfileException(errorMsg);
    }

    Map<String, List<String>> profileMap = fhirContextData.profileMap;
    return profileMap.get(resourceType);
  }

  @VisibleForTesting
  public synchronized void deRegisterFhirContexts(FhirVersionEnum fhirVersionEnum) {
    if (fhirContextMappings.containsKey(fhirVersionEnum)) {
      fhirContextMappings.remove(fhirVersionEnum);
    }
  }

  private static class FhirContextData {
    private final FhirContext fhirContext;
    private final String structureDefinitionsPath;
    private final Map<String, List<String>> profileMap;

    FhirContextData(
        FhirContext fhirContext,
        String structureDefinitionsPath,
        Map<String, List<String>> profileMap) {
      this.fhirContext = fhirContext;
      this.structureDefinitionsPath = structureDefinitionsPath;
      this.profileMap = profileMap;
    }
  }
}
