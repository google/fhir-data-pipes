package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements the ProfileMappingProvider APIs */
public class ProfileMappingProviderImpl implements ProfileMappingProvider {

  private static final Logger log = LoggerFactory.getLogger(ProfileMappingProviderImpl.class);
  private static final String JSON_EXT = ".json";
  private static String STRUCTURE_DEFINITION = "StructureDefinition";

  /**
   * This method initially loads all the default base structure definitions into the context and
   * additionally loads any custom profiles in the given structureDefinitionPath. It expects only
   * one extended profile for the given base resource type.
   *
   * <p>This method returns a map containing the mappings between the resource type and the profile
   * url which is defined for the resource type. Any extended profile defined in the
   * structureDefinitionPath will overwrite the mapped profile with the extended profile.
   *
   * @param context The context to which the profiles are added.
   * @param structureDefinitionPath the path containing the list of structure definitions to be used
   * @return the map containing the resource to profile mapping.
   */
  @Override
  public Map<String, String> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionPath) {
    List<IBaseResource> defaultDefinitions =
        context.getValidationSupport().fetchAllStructureDefinitions();
    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport(context);
    Map<String, String> profileMap = new HashMap<>();
    loadBaseStructureDefinitions(context, defaultDefinitions, profileMap, support);
    if (!Strings.isNullOrEmpty(structureDefinitionPath)) {
      loadCustomStructureDefinitions(support, context, structureDefinitionPath, profileMap);
    }
    context.setValidationSupport(support);
    return profileMap;
  }

  private Map<String, String> loadBaseStructureDefinitions(
      FhirContext context,
      List<IBaseResource> defaultDefinitions,
      Map<String, String> profileMap,
      PrePopulatedValidationSupport support) {
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

    for (IBaseResource definition : defaultDefinitions) {
      support.addStructureDefinition(definition);
      // Links the profile only if the definition belongs to a base resource
      if (isABaseResource(context, definition)) {
        mapResourceProfileMap(context, definition, profileMap);
      }
    }
    context.setValidationSupport(support);
    return resourceProfileMap;
  }

  private void loadCustomStructureDefinitions(
      PrePopulatedValidationSupport support,
      FhirContext context,
      String structureDefinitionPath,
      Map<String, String> profileMap) {
    Preconditions.checkArgument(
        context.getVersion().getVersion() == FhirVersionEnum.DSTU3
            || context.getVersion().getVersion() == FhirVersionEnum.R4);

    IParser jsonParser = context.newJsonParser();
    try {
      List<Path> paths =
          Files.walk(Paths.get(structureDefinitionPath)).collect(Collectors.toList());
      List<Path> definitionPaths = new ArrayList<>();
      paths.stream()
          .filter(f -> f.toString().endsWith(JSON_EXT))
          .forEach(
              f -> {
                definitionPaths.add(f);
              });
      for (Path definitionPath : definitionPaths) {
        IBaseResource baseResource = getResource(jsonParser, definitionPath);
        RuntimeResourceDefinition resourceDefinition = context.getResourceDefinition(baseResource);
        String resourceName = resourceDefinition.getName();
        if (resourceName.equals(STRUCTURE_DEFINITION)) {
          if (baseResource.getStructureFhirVersionEnum() == context.getVersion().getVersion()) {
            mapResourceProfileMap(context, baseResource, profileMap);
            support.addStructureDefinition(baseResource);
          }
        }
      }
    } catch (IOException e) {
      log.error("Cannot get the list of files at the directory {}", structureDefinitionPath, e);
    }
  }

  private static IBaseResource getResource(IParser jsonParser, Path definitionPath)
      throws IOException {
    try (Reader reader = Files.newBufferedReader(definitionPath, StandardCharsets.UTF_8)) {
      return jsonParser.parseResource(reader);
    }
  }

  private void mapResourceProfileMap(
      FhirContext fhirContext, IBaseResource baseResource, Map<String, String> resourceProfileMap) {
    RuntimeResourceDefinition resourceDefinition = fhirContext.getResourceDefinition(baseResource);
    String resourceName = resourceDefinition.getName();
    if (resourceName.equals(STRUCTURE_DEFINITION)) {
      String type = fetchProperty("type", resourceDefinition, baseResource);
      String baseDefinition = fetchProperty("baseDefinition", resourceDefinition, baseResource);
      String url = fetchProperty("url", resourceDefinition, baseResource);

      Preconditions.checkNotNull(url, "The url must not be null");
      if (fhirContext.getResourceTypes().contains(type)
          && baseDefinition.startsWith("http://hl7.org/fhir/StructureDefinition/")) {
        resourceProfileMap.put(type, url);
      }
    }
  }

  private String fetchProperty(
      String property, RuntimeResourceDefinition resourceDefinition, IBaseResource definition) {
    Optional<IBase> propertyValue =
        resourceDefinition.getChildByName(property).getAccessor().getFirstValueOrNull(definition);
    return propertyValue.map((t) -> ((IPrimitiveType) t).getValueAsString()).orElse(null);
  }

  /**
   * Checks if the given resource definition belongs is a StructureDefinition that belongs to a base
   * resource and not an extended definition.
   */
  private boolean isABaseResource(FhirContext fhirContext, IBaseResource definition) {
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
}
