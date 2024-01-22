/*
 * Copyright 2020-2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cerner.bunsen;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.IParser;
import com.cerner.bunsen.profiles.BaseProfileProvider;
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
import java.util.stream.Collectors;
import org.hl7.fhir.common.hapi.validation.support.PrePopulatedValidationSupport;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an implementation of {@link com.cerner.bunsen.profiles.ProfileProvider}. This class
 * is responsible for loading the structure definitions placed in a directory.
 */
public class FileProfileProvider extends BaseProfileProvider {

  private static final String JSON_EXT = ".json";
  private static final Logger log = LoggerFactory.getLogger(FileProfileProvider.class);

  /**
   * This method loads the base structure definitions from the {@link BaseProfileProvider},
   * additionally loads the structure definitions from the list of directories defined by
   * customDefinitionsPath.
   *
   * @param context The context to which the profiles are added.
   * @param customDefinitionsPath the list of paths containing the structure definitions for the
   *     profiles
   * @return the map containing the resource type and the fhir profile that has been mapped
   */
  @Override
  public Map<String, String> loadStructureDefinitions(
      FhirContext context, List<String> customDefinitionsPath) {
    Preconditions.checkState(
        customDefinitionsPath != null && !customDefinitionsPath.isEmpty(),
        "customDefinitionsPath cannot be empty");
    support = new PrePopulatedValidationSupport(context);
    Map<String, String> baseProfileMapping = super.loadStructureDefinitions(context);
    Map<String, String> customProfileMapping =
        loadCustomStructureDefinitions(support, context, customDefinitionsPath);
    context.setValidationSupport(support);
    return mergeProfileMappings(baseProfileMapping, customProfileMapping);
  }

  private static Map<String, String> loadCustomStructureDefinitions(
      PrePopulatedValidationSupport support,
      FhirContext context,
      List<String> customDefinitionsPath) {
    Preconditions.checkArgument(
        context.getVersion().getVersion() == FhirVersionEnum.DSTU3
            || context.getVersion().getVersion() == FhirVersionEnum.R4);

    Map<String, String> mappings = new HashMap<>();
    if (customDefinitionsPath != null && !customDefinitionsPath.isEmpty()) {
      IParser jsonParser = context.newJsonParser();
      for (String directory : customDefinitionsPath) {
        if (!Strings.isNullOrEmpty(directory)) {
          try {
            List<Path> paths = Files.walk(Paths.get(directory)).collect(Collectors.toList());
            List<Path> definitionPaths = new ArrayList<>();
            paths.stream()
                .filter(f -> f.toString().endsWith(JSON_EXT))
                .forEach(
                    f -> {
                      definitionPaths.add(f);
                    });
            for (Path definitionPath : definitionPaths) {
              loadFilePath(support, jsonParser, definitionPath, context, mappings);
            }
          } catch (IOException e) {
            log.error("Cannot get the list of files at the directory {}", directory, e);
          }
        }
      }
    }
    return mappings;
  }

  private static void loadFilePath(
      PrePopulatedValidationSupport support,
      IParser jsonParser,
      Path definitionPath,
      FhirContext context,
      Map<String, String> resourceProfileMap)
      throws IOException {
    try (Reader reader = Files.newBufferedReader(definitionPath, StandardCharsets.UTF_8)) {
      addStructureDefinition(support, jsonParser, reader, context, resourceProfileMap);
    }
  }

  private static void addStructureDefinition(
      PrePopulatedValidationSupport support,
      IParser jsonParser,
      Reader reader,
      FhirContext context,
      Map<String, String> resourceProfileMap) {
    IBaseResource definition = jsonParser.parseResource(reader);
    FhirVersionEnum fhirVersionEnum = context.getVersion().getVersion();

    RuntimeResourceDefinition resourceDefinition = context.getResourceDefinition(definition);
    String resourceName = resourceDefinition.getName();
    if (resourceName.equals(STRUCTURE_DEFINITION)) {
      if (definition.getStructureFhirVersionEnum() == fhirVersionEnum) {
        mapResourceToProfile(context, definition, resourceProfileMap);
        support.addStructureDefinition(definition);
      }
    }
  }
}
