package com.cerner.bunsen.definitions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
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

  /**
   * This is to extract the full path of an element based on the element traversal stack. The reason
   * for using the full path from stack is to differentiate between same types at different
   * recursion levels, e.g., when a child is dropped because of recursion max-depth while in a
   * different traversal (for the same type) that child is present. Note in both those cases the
   * element name and path would be the same hence we need to rely on the full traversal stack. As
   * an example consider these two fields: QuestionnaireResponse.item.answer.item
   * QuestionnaireResponse.item.answer.item.answer.item Both of these are `item` with "element path"
   * being `QuestionnaireResponse.item` but the `answer` field might have been dropped in the second
   * one because of recursion limits.
   *
   * @param elemName the name of the target element
   * @param stack the current traversal stack leading to `elemName`
   * @return the full path for `elemName` created using element names on the stack
   */
  public static String pathFromStack(String elemName, Deque<QualifiedPath> stack) {
    // TODO add unit-tests for this method.
    List<String> fullPath = new ArrayList<>();
    Iterator<QualifiedPath> iter = stack.descendingIterator();
    while (iter.hasNext()) {
      String path = iter.next().getElementPath();
      fullPath.add(elementName(path));
    }
    fullPath.add(elemName);
    return fullPath.stream().collect(Collectors.joining("."));
  }

  /** Creates a canonical element name from the element path. */
  public static String elementName(String elementPath) {

    String suffix = elementPath.substring(elementPath.lastIndexOf('.') + 1);

    return suffix.endsWith("[x]") ? suffix.substring(0, suffix.length() - 3) : suffix;
  }
}
