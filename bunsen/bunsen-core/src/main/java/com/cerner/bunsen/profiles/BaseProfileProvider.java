package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.support.IValidationSupport;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class defining the default implementations for {@link ProfileProvider} methods. */
public abstract class BaseProfileProvider implements ProfileProvider {

  private static final Logger log = LoggerFactory.getLogger(BaseProfileProvider.class);

  protected static String STRUCTURE_DEFINITION = "StructureDefinition";

  protected PrePopulatedValidationSupport support;

  /**
   * This method loads the default structure definitions for all the FHIR resource types.
   *
   * @param context The context to which the profiles are added.
   * @return the map containing the resource type and the profile being supported
   */
  @Override
  public Map<String, String> loadStructureDefinitions(FhirContext context) {
    Map<String, String> resourceProfileMap = new HashMap<>();
    if (context.getVersion().getVersion() != FhirVersionEnum.DSTU3
        && context.getVersion().getVersion() != FhirVersionEnum.R4) {
      log.warn(
          "Skipped loading FHIR profiles for FhirContext version {} as it is not {} or {} ",
          context.getVersion().getVersion(),
          FhirVersionEnum.DSTU3,
          FhirVersionEnum.R4);
      return resourceProfileMap;
    }

    IValidationSupport defaultSupport = context.getValidationSupport();
    if (support == null) support = new PrePopulatedValidationSupport(context);
    List<IBaseResource> defaultDefinitions = defaultSupport.fetchAllStructureDefinitions();
    for (IBaseResource definition : defaultDefinitions) {
      support.addStructureDefinition(definition);
      // Links the profile only if the definition belongs to a base resource
      if (isABaseResource(context, definition)) {
        mapResourceToProfile(context, definition, resourceProfileMap);
      }
    }
    context.setValidationSupport(support);
    return resourceProfileMap;
  }

  /** This method maps the resourceType in the definition to the URL profile. */
  protected static void mapResourceToProfile(
      FhirContext fhirContext, IBaseResource definition, Map<String, String> resourceProfileMap) {

    RuntimeResourceDefinition resourceDefinition = fhirContext.getResourceDefinition(definition);
    String resourceName = resourceDefinition.getName();
    if (resourceName.equals(STRUCTURE_DEFINITION)) {
      String type = fetchProperty("type", resourceDefinition, definition);
      String baseDefinition = fetchProperty("baseDefinition", resourceDefinition, definition);
      String url = fetchProperty("url", resourceDefinition, definition);

      Preconditions.checkNotNull(url, "The url must not be null");

      if (fhirContext.getResourceTypes().contains(type)
          && baseDefinition.startsWith("http://hl7.org/fhir/StructureDefinition/")) {
        resourceProfileMap.put(type, url);
      }
    }
  }

  /**
   * Checks if the given resource definition belongs is a StructureDefinition that belongs to a base
   * resource and not an extended definition.
   */
  private static boolean isABaseResource(FhirContext fhirContext, IBaseResource definition) {
    RuntimeResourceDefinition resourceDefinition = fhirContext.getResourceDefinition(definition);
    String resourceName = resourceDefinition.getName();
    if (resourceName.equals(STRUCTURE_DEFINITION)) {
      String type = fetchProperty("type", resourceDefinition, definition);
      String baseDefinition = fetchProperty("baseDefinition", resourceDefinition, definition);
      if (fhirContext.getResourceTypes().contains(type)
          && baseDefinition.equalsIgnoreCase(
              "http://hl7.org/fhir/StructureDefinition/DomainResource")) {
        return true;
      }
    }
    return false;
  }

  private static String fetchProperty(
      String property, RuntimeResourceDefinition resourceDefinition, IBaseResource definition) {
    Optional<IBase> propertyValue =
        resourceDefinition.getChildByName(property).getAccessor().getFirstValueOrNull(definition);
    return propertyValue.map((t) -> ((IPrimitiveType) t).getValueAsString()).orElse(null);
  }

  protected Map<String, String> mergeProfileMappings(
      Map<String, String> profileMappings1, Map<String, String> profileMappings2) {
    Map<String, String> mergedProfileMap = new HashMap<>(profileMappings1);
    if (profileMappings2 != null && !profileMappings2.isEmpty()) {
      for (Entry<String, String> entry : profileMappings2.entrySet()) {
        mergedProfileMap.put(entry.getKey(), entry.getValue());
      }
    }
    return mergedProfileMap;
  }
}
