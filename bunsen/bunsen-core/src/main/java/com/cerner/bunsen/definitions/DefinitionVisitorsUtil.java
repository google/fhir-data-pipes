package com.cerner.bunsen.definitions;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Util class that provides helper methods for concrete visitors that implement {@link
 * DefinitionVisitor} interface.
 */
public class DefinitionVisitorsUtil {

  private static final Pattern STRUCTURE_URL_PATTERN =
      Pattern.compile("http:\\/\\/hl7.org\\/fhir(\\/.*)?\\/StructureDefinition\\/([^\\/]*)$");

  private static final String CHILDREN_TOKEN = "_CHILDREN_";

  /**
   * Helper method to convert a given element path that's delimited by period to a concatenated
   * string in title case.
   *
   * @param elementPath the element path delimited by period to be converted
   * @return a converted {@link String}
   */
  public static String recordNameFor(String elementPath) {

    return Arrays.stream(elementPath.split("\\."))
        .map(StringUtils::capitalize)
        .reduce(String::concat)
        .get();
  }

  public static <T> String recordNameFor(String elementPath, List<StructureField<T>> children) {
    return recordNameFor(elementPath)
        + CHILDREN_TOKEN + children.stream().map(
            c -> c.fieldName()).collect(Collectors.joining("_"));
  }

  public static String getBaseName(String name) {
    int tokenInd = name.indexOf(CHILDREN_TOKEN);
    if (tokenInd >= 0) {
      return name.substring(0, tokenInd);
    }
    return name;
  }

  /**
   * Helper method that returns a fully qualified namespace for a given StructureDefinition url.
   *
   * @param basePackage the base package to be used as prefix in the returned namespace
   * @param structureDefinitionUrl the StructureDefinition url
   * @return a fully qualified namespace
   */
  public static String namespaceFor(String basePackage, String structureDefinitionUrl) {

    Matcher matcher = STRUCTURE_URL_PATTERN.matcher(structureDefinitionUrl);

    if (matcher.matches()) {

      String profile = matcher.group(1);

      if (profile != null && profile.length() > 0) {

        String subPackage = profile.replaceAll("/", ".");

        return basePackage + subPackage;

      } else {
        return basePackage;
      }

    } else {
      throw new IllegalArgumentException(
          "Unrecognized structure definition URL: " + structureDefinitionUrl);
    }
  }

}
