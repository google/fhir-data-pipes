package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
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
  private static ProfileMapperFhirContexts instance;
  private ProfileMappingProvider profileMappingProvider;

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
   * @param structureDefinitionsPath The path containing the custom structure definitions
   * @return the FhirContext
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  public synchronized FhirContext contextFor(
      FhirVersionEnum fhirVersion, @Nullable String structureDefinitionsPath)
      throws ProfileMapperException {
    structureDefinitionsPath = Strings.nullToEmpty(structureDefinitionsPath);
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      FhirContext context = new FhirContext(fhirVersion);
      Map<String, String> profileMap = loadStructureDefinitions(context, structureDefinitionsPath);
      fhirContextData = new FhirContextData(context, structureDefinitionsPath, profileMap);
      fhirContextMappings.put(fhirVersion, fhirContextData);
    } else {
      if (!Objects.equals(structureDefinitionsPath, fhirContextData.structureDefinitionsPath)) {
        String errorMsg =
            String.format(
                "Failed to initialise FhirContext with structureDefinitionsPath=%s, it is already"
                    + " initialised with a different structureDefinitionsPath=%s",
                structureDefinitionsPath, fhirContextData.structureDefinitionsPath);
        logger.error(errorMsg);
        throw new ProfileMapperException(errorMsg);
      }
    }
    return fhirContextData.fhirContext;
  }

  /**
   * Loads base structure definitions and also the custom structure definitions present in the given
   * {@code structureDefinitionsPath} path.
   *
   * @param context the context into which the structure definitions are loaded
   * @param structureDefinitionsPath the path containing the custom structure definitions
   * @return the map containing the resource to profile url mappings
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  private Map<String, String> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionsPath)
      throws ProfileMapperException {
    return profileMappingProvider.loadStructureDefinitions(context, structureDefinitionsPath);
  }

  /**
   * Returns the mapped profile url for the given resourceType. The base profile url will be
   * returned by default, unless it is overridden by a custom profile url during initialisation of
   * fhirContext (in which case the custom profile url is returned).
   *
   * @param fhirVersion the fhir version for which the mapping needs to be returned
   * @param resourceType the resource type
   * @return the profile url
   * @throws ProfileMapperException if the FhirContext is not initialised for the given fhirVersion
   */
  public String getMappedProfileForResource(FhirVersionEnum fhirVersion, String resourceType)
      throws ProfileMapperException {
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      String errorMsg =
          String.format(
              "The fhirContext has not been initialised yet for version=%s, Please initialise the"
                  + " fhirContext using the method contextFor(FhirVersionEnum fhirVersion,"
                  + " @Nullable String structureDefinitionsPath) before fetching the mapped"
                  + " profile.",
              fhirVersion);
      logger.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }

    Map<String, String> profileMap = fhirContextData.profileMap;
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
    private final Map<String, String> profileMap;

    FhirContextData(
        FhirContext fhirContext, String structureDefinitionsPath, Map<String, String> profileMap) {
      this.fhirContext = fhirContext;
      this.structureDefinitionsPath = structureDefinitionsPath;
      this.profileMap = profileMap;
    }
  }
}
