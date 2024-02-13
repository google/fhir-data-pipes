package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
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
 * This class loads the Fhir contexts and caches them for reuse. It loads the profiles that
 * implement the {@link ProfileMappingProvider}.
 */
public class ProfileMapperFhirContexts {

  private static final Logger logger =
      LoggerFactory.getLogger(ProfileMapperFhirContexts.class.getName());
  private final Map<FhirVersionEnum, FhirContextData> fhirContextMappings = new HashMap();
  private static ProfileMapperFhirContexts instance;

  public static ProfileMapperFhirContexts getInstance() {
    synchronized (ProfileMapperFhirContexts.class) {
      if (instance == null) {
        instance = new ProfileMapperFhirContexts();
      }
      return instance;
    }
  }

  /**
   * Returns the FHIR context for the given version and the structureDefinitionPath. It loads the
   * profiles defined in the structureDefinitionPath. This method caches the Fhir context for the
   * given input combination and returns the same FhirContext for any further invocations of similar
   * arguments. It returns an error if the method is called with a different structureDefinitionPath
   * for the given fhirVersion from second time onwards.
   *
   * @param fhirVersion the version of FHIR to use
   * @param structureDefinitionPath The path containing the custom structure definitions
   * @return the FhirContext
   */
  public FhirContext contextFor(
      FhirVersionEnum fhirVersion, @Nullable String structureDefinitionPath) {
    structureDefinitionPath = Strings.nullToEmpty(structureDefinitionPath);
    synchronized (fhirContextMappings) {
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
          throw new IllegalStateException(errorMsg);
        }
      }
      return fhirContextData.fhirContext;
    }
  }

  /**
   * Loads the custom structure definitions present in the given {@code structureDefinitionPaths}
   * paths.
   *
   * @param context the context into which the structure definitions are loaded
   */
  private Map<String, String> loadProfiles(
      FhirContext context, @Nullable String structureDefinitionPath) {
    ProfileMappingProvider profileMappingProvider = new ProfileMappingProviderImpl();
    return profileMappingProvider.loadStructureDefinitions(context, structureDefinitionPath);
  }

  public String getMappedProfileForResource(FhirVersionEnum fhirVersion, String resourceType) {
    FhirContextData fhirContextData = fhirContextMappings.get(fhirVersion);
    if (fhirContextData == null) {
      String errorMsg =
          String.format("The fhirContext has not yet been initialised for version=%s", fhirVersion);
      logger.error(errorMsg);
      throw new IllegalStateException(errorMsg);
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
