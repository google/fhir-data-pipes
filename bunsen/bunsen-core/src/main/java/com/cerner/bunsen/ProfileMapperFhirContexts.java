package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.cerner.bunsen.profiles.ProfileMappingProvider;
import com.cerner.bunsen.profiles.ProfileMappingProviderImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.HashMap;
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
  private final Map<FhirVersionEnum, FhirContextData> fhirContextMappings = new HashMap();
  private static ProfileMapperFhirContexts instance;

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
   * Returns the FHIR context for the given version and the structureDefinitionPath. For the given
   * fhirVersion, this method loads the all the base profiles and also the custom profiles defined
   * in the structureDefinitionPath. This method caches the Fhir context for the given input
   * combination of fhirVersion and structureDefinitionPath, and returns the same FhirContext for
   * any further invocations of similar arguments. It returns an error if the method is called with
   * a different structureDefinitionPath for the same fhirVersion earlier.
   *
   * @param fhirVersion the version of FHIR to use
   * @param structureDefinitionPath The path containing the custom structure definitions
   * @return the FhirContext
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  public synchronized FhirContext contextFor(
      FhirVersionEnum fhirVersion, @Nullable String structureDefinitionPath)
      throws ProfileMapperException {
    structureDefinitionPath = Strings.nullToEmpty(structureDefinitionPath);
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      FhirContext context = new FhirContext(fhirVersion);
      Map<String, String> profileMap = loadProfiles(context, structureDefinitionPath);
      fhirContextData = new FhirContextData(context, structureDefinitionPath, profileMap);
      fhirContextMappings.put(fhirVersion, fhirContextData);
    } else {
      if (!Objects.equals(structureDefinitionPath, fhirContextData.structureDefinitionPath)) {
        String errorMsg =
            String.format(
                "FhirStructure has already been initialised with a different "
                    + "structureDefinitionPath=%s",
                fhirContextData.structureDefinitionPath);
        logger.error(errorMsg);
        throw new ProfileMapperException(errorMsg);
      }
    }
    return fhirContextData.fhirContext;
  }

  /**
   * Loads base structure definitions and also the custom structure definitions present in the given
   * {@code structureDefinitionPath} path.
   *
   * @param context the context into which the structure definitions are loaded
   * @param structureDefinitionPath the path containing the custom structure definitions
   * @return the map containing the resource to profile url mappings
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  private Map<String, String> loadProfiles(
      FhirContext context, @Nullable String structureDefinitionPath) throws ProfileMapperException {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProviderImpl();
    return profileMappingProvider.loadStructureDefinitions(context, structureDefinitionPath);
  }

  /**
   * Returns the mapped profile url for the given resourceType
   *
   * @param fhirVersion the fhir version for which the mapping needs to be returned
   * @param resourceType the resource type
   * @return the profile url
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  public String getMappedProfileForResource(FhirVersionEnum fhirVersion, String resourceType)
      throws ProfileMapperException {
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      String errorMsg =
          String.format("The fhirContext has not yet been initialised for version=%s", fhirVersion);
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

  static class FhirContextData {
    FhirContext fhirContext;
    String structureDefinitionPath;
    Map<String, String> profileMap;

    FhirContextData(
        FhirContext fhirContext, String structureDefinitionPath, Map<String, String> profileMap) {
      this.fhirContext = fhirContext;
      this.structureDefinitionPath = structureDefinitionPath;
      this.profileMap = profileMap;
    }
  }
}
