package com.cerner.bunsen.avro.tools;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.cerner.bunsen.exception.ProfileException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;

/** Simple utility class to generate avro schemas for a given set of resource types. */
public class GenerateSchemas {

  public static final String DELIMITER = ";";

  /**
   * Main entrypoint for schema generation tool.
   *
   * @param args the output file followed by a list of resource type urls
   */
  public static void main(String[] args) throws ProfileException {

    if (args.length < 4) {
      System.out.println(
          "The arguments should be of the format: <output file> <fhirVersion> <structure"
              + " definitions path> resourceTypeUrls...");
      System.out.println("Example:");

      System.out.println(
          " my_schemas.avsc DSTU3 /stu3-us-core-definitions "
              + "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient "
              + "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition");

      System.out.println();
      System.out.println(
          "The resulting avsc file then can be used to generate Java classes "
              + "using avro-tools, for example:");
      System.out.println("  avro-tools compile protocol my_schemas.avsc <target_directory>");
    }

    File outputFile = new File(args[0]);

    if (outputFile.exists()) {
      String errorMsg = "File " + outputFile.getName() + " already exists.";
      System.out.println(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }

    FhirVersionEnum fhirVersionEnum = FhirVersionEnum.forVersionString(args[1]);
    String structureDefinitionsPath = args[2];
    Map<String, List<String>> resourceTypeUrls =
        Arrays.stream(args)
            .skip(3)
            .collect(
                Collectors.toMap(
                    item -> item.split(DELIMITER)[0], item -> generateContainedUrls(item)));

    FhirContext fhirContext =
        ProfileMapperFhirContexts.getInstance()
            .contextFor(fhirVersionEnum, structureDefinitionsPath);
    List<Schema> schemas = AvroConverter.generateSchemas(fhirContext, resourceTypeUrls, 1);

    // Wrap the schemas in a protocol to simplify the invocation of the compiler.
    Protocol protocol =
        new Protocol(
            "FhirGeneratedSchemas",
            "Avro schemas generated from FHIR StructureDefinitions",
            "com.cerner.bunsen.avro");

    protocol.setTypes(schemas);

    try {
      Files.write(outputFile.toPath(), protocol.toString(true).getBytes());
    } catch (IOException exception) {
      System.out.println("Unable to write file " + outputFile.getPath());
      exception.printStackTrace();
    }
  }

  /**
   * Helper function to extract contained resources from resource string.
   *
   * @param key the string containing resource url(s)
   * @return the list of contained urls
   */
  public static List<String> generateContainedUrls(String key) {
    if (!key.contains(DELIMITER)) {
      return Collections.emptyList();
    }
    String[] splitKey = key.split(DELIMITER);
    String[] valList = Arrays.copyOfRange(splitKey, 1, splitKey.length);
    return Arrays.asList(valList);
  }
}
