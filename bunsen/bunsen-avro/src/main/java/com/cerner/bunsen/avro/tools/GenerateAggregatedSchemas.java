package com.cerner.bunsen.avro.tools;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

/** This class can be used to generate aggregated avro schemas for the FHIR profile extensions. */
public class GenerateAggregatedSchemas {

  private static final String FHIR_VERSION = "fhirVersion";
  private static final String STRUCTURE_DEFINITIONS_PATH = "structureDefinitionsPath";
  private static final String IS_CLASSPATH = "isClasspath";
  private static final String RESOURCE_TYPES = "resourceTypes";
  private static final String OUTPUT_DIR = "outputDir";

  public static void main(String[] args) throws ProfileException {
    try {
      Map<String, String> pairs = convertArgsToPairs(args);
      String fhirVersionString = pairs.get(FHIR_VERSION);
      FhirVersionEnum fhirVersionEnum =
          Strings.isNullOrEmpty(fhirVersionString)
              ? null
              : FhirVersionEnum.forVersionString(fhirVersionString);
      String structureDefinitionsPath = pairs.get(STRUCTURE_DEFINITIONS_PATH);
      boolean isClasspath = Boolean.valueOf(pairs.get(IS_CLASSPATH));
      String resourceTypesString = pairs.get(RESOURCE_TYPES);
      List<String> resourceTypes = new ArrayList<>();
      if (!Strings.isNullOrEmpty(resourceTypesString)) {
        resourceTypes = Arrays.stream(resourceTypesString.split(",")).collect(Collectors.toList());
      }
      String outputDir = pairs.get(OUTPUT_DIR);
      generateAggregatedSchemas(
          fhirVersionEnum, structureDefinitionsPath, resourceTypes, outputDir);
    } catch (Exception e) {
      System.out.println("Unable to generate aggregated schema, error=" + e.getMessage());
      System.out.println(
          "The arguments should be of the format: fhirVersion=<fhirVersion>"
              + " structureDefinitionsPath=<Path for structure definitions>"
              + " resourceTypes=<Comma Separated resource types> outputDir=<Path for generated"
              + " schemas>");
      System.out.println("Example Arguments:");
      System.out.println(
          "fhirVersion=R4"
              + " structureDefinitionsPath=classpath:/r4-us-core-definitions"
              + " resourceTypes=Patient,Observation outputDir=/usr/tmp");
      System.out.println();
      e.printStackTrace();
    }
  }

  private static Map<String, String> convertArgsToPairs(String[] args) {
    HashMap<String, String> params = new HashMap<>();
    for (String arg : args) {
      String[] splitFromEqual = arg.split("=");
      if (splitFromEqual.length != 2) {
        throw new IllegalArgumentException(
            String.format("Invalid key=value params, pair: %s is invalid", arg));
      }
      params.put(splitFromEqual[0], splitFromEqual[1]);
    }
    return params;
  }

  private static void generateAggregatedSchemas(
      FhirVersionEnum fhirVersionEnum,
      String structureDefinitionsPath,
      List<String> resourceTypes,
      String outputDir)
      throws ProfileException, IOException {
    Preconditions.checkNotNull(fhirVersionEnum, String.format("%s cannot be empty", FHIR_VERSION));
    Preconditions.checkNotNull(
        structureDefinitionsPath, String.format("%s cannot be empty", STRUCTURE_DEFINITIONS_PATH));
    Preconditions.checkState(
        resourceTypes != null && resourceTypes.size() > 1,
        String.format("%s cannot be empty", RESOURCE_TYPES));
    Preconditions.checkNotNull(outputDir, String.format("%s cannot be empty", OUTPUT_DIR));
    FhirContext fhirContext;
    fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFor(fhirVersionEnum, structureDefinitionsPath);

    for (String resourceType : resourceTypes) {
      List<String> resourceTypeURLs =
          ProfileMapperFhirContexts.getInstance()
              .getMappedProfilesForResource(FhirVersionEnum.R4, resourceType);
      AvroConverter aggregatedConverter =
          AvroConverter.forResources(fhirContext, resourceTypeURLs, 1, false);
      createOutputFile(resourceType, aggregatedConverter.getSchema(), outputDir);
    }
  }

  private static void createOutputFile(String resourceType, Schema schema, String outputDir)
      throws IOException {
    File outputDirFile = new File(outputDir);
    outputDirFile.createNewFile();
    File resourceFile = new File(outputDirFile, resourceType + ".json");
    Files.write(resourceFile.toPath(), schema.toString(true).getBytes());
  }
}
