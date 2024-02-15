package com.cerner.bunsen.profiles;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.exception.ProfileMapperException;
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
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  @Override
  public Map<String, String> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionPath) throws ProfileMapperException {

    if (context.getVersion().getVersion() != FhirVersionEnum.DSTU3
        && context.getVersion().getVersion() != FhirVersionEnum.R4) {
      log.warn(
          "Skipped loading FHIR profiles for FhirContext version {} as it is not {} or {} ",
          context.getVersion().getVersion(),
          FhirVersionEnum.DSTU3,
          FhirVersionEnum.R4);
      return new HashMap<>();
    }

    List<IBaseResource> defaultDefinitions =
        context.getValidationSupport().fetchAllStructureDefinitions();
    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport(context);
    Map<String, String> baseResourcePofileMap =
        loadBaseStructureDefinitions(context, defaultDefinitions, support);
    if (!Strings.isNullOrEmpty(structureDefinitionPath)) {
      Map<String, String> customResourceProfileMap =
          loadCustomStructureDefinitions(support, context, structureDefinitionPath);
      // Overwrite the profiles for the resources with the custom profiles
      baseResourcePofileMap.putAll(customResourceProfileMap);
    }
    context.setValidationSupport(support);
    return baseResourcePofileMap;
  }

  private Map<String, String> loadBaseStructureDefinitions(
      FhirContext context,
      List<IBaseResource> defaultDefinitions,
      PrePopulatedValidationSupport support) {
    Map<String, String> resourceProfileMap = new HashMap<>();
    for (IBaseResource definition : defaultDefinitions) {
      support.addStructureDefinition(definition);
      // Links the profile only if the definition belongs to a base resource
      if (isABaseResource(context, definition)) {
        RuntimeResourceDefinition resourceDefinition = context.getResourceDefinition(definition);
        String type = fetchProperty("type", resourceDefinition, definition);
        String url = fetchProperty("url", resourceDefinition, definition);
        resourceProfileMap.put(type, url);
      }
    }
    context.setValidationSupport(support);
    return resourceProfileMap;
  }

  /**
   * This method loads the custom structure definitions defined in the path structureDefinitionPath.
   * It fails if any duplicate profile is defined for the same resource type.
   */
  private Map<String, String> loadCustomStructureDefinitions(
      PrePopulatedValidationSupport support, FhirContext context, String structureDefinitionPath)
      throws ProfileMapperException {
    Map<String, String> resourceProfileMap = new HashMap<>();
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
        addDefinitionAndMapping(context, baseResource, support, resourceProfileMap);
      }
    } catch (IOException e) {
      log.error("Cannot get the list of files at the directory {}", structureDefinitionPath, e);
    }
    return resourceProfileMap;
  }

  private void addDefinitionAndMapping(
      FhirContext context,
      IBaseResource baseResource,
      PrePopulatedValidationSupport support,
      Map<String, String> resourceProfileMap)
      throws ProfileMapperException {
    RuntimeResourceDefinition resourceDefinition = context.getResourceDefinition(baseResource);
    String resourceName = resourceDefinition.getName();
    if (resourceName.equals(STRUCTURE_DEFINITION)
        && baseResource.getStructureFhirVersionEnum() == context.getVersion().getVersion()) {
      String type = fetchProperty("type", resourceDefinition, baseResource);
      String baseDefinition = fetchProperty("baseDefinition", resourceDefinition, baseResource);
      String url = fetchProperty("url", resourceDefinition, baseResource);

      Preconditions.checkNotNull(url, "The url must not be null");
      if (resourceProfileMap.containsKey(type)) {
        String errorMsg =
            String.format(
                "Resource has already been mapped to a custom profile, resourceType=%s", type);
        log.error(errorMsg);
        throw new ProfileMapperException(errorMsg);
      }
      if (context.getResourceTypes().contains(type)
          && baseDefinition.startsWith("http://hl7.org/fhir/StructureDefinition/")) {
        resourceProfileMap.put(type, url);
      }
      support.addStructureDefinition(baseResource);
    }
  }

  private static IBaseResource getResource(IParser jsonParser, Path definitionPath)
      throws IOException {
    try (Reader reader = Files.newBufferedReader(definitionPath, StandardCharsets.UTF_8)) {
      return jsonParser.parseResource(reader);
    }
  }

  private String fetchProperty(
      String property, RuntimeResourceDefinition resourceDefinition, IBaseResource definition) {
    Optional<IBase> propertyValue =
        resourceDefinition.getChildByName(property).getAccessor().getFirstValueOrNull(definition);
    return propertyValue.map((t) -> ((IPrimitiveType) t).getValueAsString()).orElse(null);
  }

  /**
   * Checks if the given resource definition is a StructureDefinition that belongs to a base
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
