package com.cerner.bunsen.avro.tools;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
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
   * @return the OS status code
   */
  public static int main(String[] args) {

    if (args.length < 2) {
      System.out.println("Usage: GenerateSchemas <output file> resourceTypeUrls...");
      System.out.println("Example:");

      System.out.println(
          "  GenerateSchemas my_schemas.avsc "
              + "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient "
              + "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition");

      System.out.println();
      System.out.println(
          "The resulting avsc file then can be used to generate Java classes "
              + "using avro-tools, for example:");
      System.out.println("  avro-tools compile protocol my_schemas.avsc <target_directory>");

      return 1;
    }

    File outputFile = new File(args[0]);

    if (outputFile.exists()) {

      System.out.println("File " + outputFile.getName() + " already exists.");
      return 1;
    }

    Map<String, List<String>> resourceTypeUrls =
        Arrays.stream(args)
            .skip(1)
            .collect(
                Collectors.toMap(
                    item -> item.split(DELIMITER)[0], item -> generateContainedUrls(item)));

    List<Schema> schemas = AvroConverter.generateSchemas(FhirContexts.forStu3(), resourceTypeUrls);

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
      return 1;
    }

    return 0;
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
