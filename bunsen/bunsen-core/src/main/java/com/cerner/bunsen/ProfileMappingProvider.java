package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
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

/**
 * Class which helps in loading StructureDefinitions into the FhirContext and create the mappings
 * for the resource type to profiles
 */
class ProfileMappingProvider {

  private static final Logger log = LoggerFactory.getLogger(ProfileMappingProvider.class);
  private static final String JSON_EXT = ".json";
  private static String STRUCTURE_DEFINITION = "StructureDefinition";

  /**
   * This method initially loads all the default base structure definitions into the context and
   * additionally loads any custom profiles in the given structureDefinitionsPath. It expects only
   * one extended profile for the given base resource type.
   *
   * <p>This method returns a map containing the mappings between the resource type and the profile
   * url which is defined for the resource type. Any extended profile defined in the
   * structureDefinitionsPath will overwrite the mapped profile with the extended profile.
   *
   * @param context The context to which the profiles are added.
   * @param structureDefinitionsPath the path containing the list of structure definitions to be
   *     used
   * @return the map containing the resource type to profile mapping.
   * @throws ProfileMapperException if there are any errors while loading and mapping the structure
   *     definitions
   */
  Map<String, String> loadStructureDefinitions(
      FhirContext context, @Nullable String structureDefinitionsPath, boolean isClasspath)
      throws ProfileMapperException {

    // TODO: Add support for other versions (R4B and R5) and then remove this constraint
    // https://github.com/google/fhir-data-pipes/issues/958
    if (context.getVersion().getVersion() != FhirVersionEnum.DSTU3
        && context.getVersion().getVersion() != FhirVersionEnum.R4) {
      String errorMsg =
          String.format(
              "Cannot load FHIR profiles for FhirContext version %s as it is not %s or %s ",
              context.getVersion().getVersion(), FhirVersionEnum.DSTU3, FhirVersionEnum.R4);
      log.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }

    PrePopulatedValidationSupport support = new PrePopulatedValidationSupport(context);
    Map<String, String> baseResourcePofileMap = loadBaseStructureDefinitions(context, support);
    if (!Strings.isNullOrEmpty(structureDefinitionsPath)) {
      Map<String, String> customResourceProfileMap =
          loadCustomStructureDefinitions(context, support, structureDefinitionsPath, isClasspath);
      // Overwrite the profiles for the resources with the custom profiles
      baseResourcePofileMap.putAll(customResourceProfileMap);
    }
    context.setValidationSupport(support);
    return baseResourcePofileMap;
  }

  private Map<String, String> loadBaseStructureDefinitions(
      FhirContext context, PrePopulatedValidationSupport support) {
    Map<String, String> resourceProfileMap = new HashMap<>();
    List<IBaseResource> defaultDefinitions =
        context.getValidationSupport().fetchAllStructureDefinitions();
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
   * This method loads the custom structure definitions defined in the path
   * structureDefinitionsPath. It fails if any duplicate profile is defined for the same resource
   * type.
   */
  private Map<String, String> loadCustomStructureDefinitions(
      FhirContext context,
      PrePopulatedValidationSupport support,
      String structureDefinitionsPath,
      boolean isClasspath)
      throws ProfileMapperException {
    Map<String, String> resourceProfileMap = new HashMap<>();
    IParser jsonParser = context.newJsonParser();
    try {
      Path path =
          isClasspath
              ? getDirectoryPathFromClasspath(structureDefinitionsPath)
              : Paths.get(structureDefinitionsPath);
      List<Path> paths = Files.walk(path).collect(Collectors.toList());
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
    } catch (IOException | URISyntaxException e) {
      String errorMsg =
          String.format(
              "Cannot get the list of files at the directory=%s, classpath=%s, error=%s",
              structureDefinitionsPath, isClasspath, e.getMessage());
      log.error(errorMsg, e);
      throw new ProfileMapperException(errorMsg);
    }
    return resourceProfileMap;
  }

  private Path getDirectoryPathFromClasspath(String classpath)
      throws URISyntaxException, IOException, ProfileMapperException {
    URL resourceURL = getClass().getResource(classpath);
    if (resourceURL == null) {
      String errorMsg = String.format("the classpath url=%s does not exist", classpath);
      log.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }

    URI uri = resourceURL.toURI();
    // This is to make sure that the FileSystem is initialised for zip files, as the API
    // Paths.get() does not create automatically.
    if (uri.getScheme().equals("jar")) {
      Map<String, String> env = new HashMap<>();
      env.put("create", "true");
      try {
        FileSystem fileSystem = FileSystems.getFileSystem(uri);
        if (!fileSystem.isOpen()) {
          FileSystems.newFileSystem(uri, env);
        }
      } catch (FileSystemNotFoundException e) {
        FileSystems.newFileSystem(uri, env);
      }
    }
    return Paths.get(resourceURL.toURI());
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
                "ResourceType=%s has already been mapped to a custom profile, current profile"
                    + " mapped=%s, new profile trying to be mapped=%s",
                type, resourceProfileMap.get(type), url);
        log.error(errorMsg);
        throw new ProfileMapperException(errorMsg);
      }

      if (context.getResourceTypes().contains(type)) {
        if (!baseDefinition.startsWith("http://hl7.org/fhir/StructureDefinition/")) {
          String errorMsg =
              String.format(
                  "Profiles which extend the extended profiles are not supported ResourceType=%s"
                      + " with profile url=%s cannot be supported",
                  type, baseDefinition);
          log.error(errorMsg);
          throw new ProfileMapperException(errorMsg);
        }
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
