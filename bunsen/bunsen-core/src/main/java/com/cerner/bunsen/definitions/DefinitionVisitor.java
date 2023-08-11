package com.cerner.bunsen.definitions;

import java.util.List;
import java.util.Map;

/**
 * Visitor for each field in a FHIR StructureDefinition.
 *
 * @param <T> the type produced by the visitor.
 */
public interface DefinitionVisitor<T> {

  /**
   * Visits a primitive type.
   *
   * @param elementName the element to visit.
   * @param primitiveType the FHIR type of the primitive.
   * @return the visitor result.
   */
  T visitPrimitive(String elementName, String primitiveType);

  /**
   * Visits a composite type.
   *
   * @param elementName the element to visit.
   * @param elementPath the FHIR path to the element.
   * @param baseType the type of the composite type.
   * @param elementTypeUrl the URL of the resource containing the element.
   * @param children the composite type's children.
   * @return the visitor result.
   */
  T visitComposite(
      String elementName,
      String elementPath,
      String baseType,
      String elementTypeUrl,
      List<StructureField<T>> children);

  /**
   * Visits a container type.
   *
   * @param elementPath the FHIR path to the element.
   * @param baseType the type of the container type.
   * @param contained the map of types to their contained elements.
   * @return the visitor result.
   */
  T visitContained(String elementPath, String baseType, Map<String, StructureField<T>> contained);

  /**
   * Visits a reference type. The element name in itself is not sufficient as similar element names
   * at different places cause conflict and hence we would need full schema path to differentiate
   * between such conflicting reference names.
   *
   * @param elementName the full schema path of element which needs to be visited.
   * @param referenceTypes the types of resource that can be referenced
   * @param children the child fields of the reference
   * @return the visitor result.
   */
  T visitReference(
      String elementName, List<String> referenceTypes, List<StructureField<T>> children);

  /**
   * Visits a non-leaf extension.
   *
   * @param elementName the element to visit.
   * @param extensionUrl the URL of the extension.
   * @param children the children of the extension
   * @return the visitor result.
   */
  T visitParentExtension(String elementName, String extensionUrl, List<StructureField<T>> children);

  /**
   * Visits a leaf extension, which contains some value.
   *
   * @param elementName the element to visit.
   * @param extensionUrl the URL of the extension.
   * @param element the children of the extension.
   * @return the visitor result.
   */
  T visitLeafExtension(String elementName, String extensionUrl, T element);

  /**
   * Visits a multi-valued element.
   *
   * @param elementName the element to visit.
   * @param arrayElement the visitor result for a single element of the array.
   * @return the visitor result.
   */
  T visitMultiValued(String elementName, T arrayElement);

  /**
   * Visits a choice type.
   *
   * @param elementName the element to visit.
   * @param fhirToChoiceTypes a map of the choice type with the returned children.
   * @return the visitor result.
   */
  T visitChoice(String elementName, Map<String, T> fhirToChoiceTypes);

  /**
   * Returns the maximum depth to use for recursive structures of the given type.
   *
   * @param elementTypeUrl the element type that is recursive
   * @param path the path to the element that is recursive
   * @return the depth the visitor should recur to.
   */
  int getMaxDepth(String elementTypeUrl, String path);
}
