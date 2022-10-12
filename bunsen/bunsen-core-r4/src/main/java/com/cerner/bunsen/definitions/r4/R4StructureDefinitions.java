package com.cerner.bunsen.definitions.r4;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.IValidationSupport;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.QualifiedPath;
import com.cerner.bunsen.definitions.StructureDefinitions;
import com.cerner.bunsen.definitions.StructureField;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.ElementDefinition;
import org.hl7.fhir.r4.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: A significant part of this is similar to Stu3StructureDefinitions which we should refactor.
// This is non-trivial because FHIR ElementDefinition objects do not share the same interface for
// different FHIR versions.

/**
 * {@link StructureDefinitions} implementation for FHIR R4.
 */
public class R4StructureDefinitions extends StructureDefinitions {

  private static final Logger log = LoggerFactory.getLogger(R4StructureDefinitions.class);

  private static final FhirConversionSupport CONVERSION_SUPPORT = new R4FhirConversionSupport();

  public R4StructureDefinitions(FhirContext context) {

    super(context);
  }

  private List<ElementDefinition> getChildren(ElementDefinition parent,
      List<ElementDefinition> definitions) {

    if (parent.getContentReference() != null) {

      if (!parent.getContentReference().startsWith("#")) {

        throw new IllegalStateException("Non-local references are not yet supported");
      }

      // Remove the leading hash (#) to get the referenced type.
      String referencedType = parent.getContentReference().substring(1);

      // Find the actual type to use.
      parent = definitions.stream()
          .filter(definition -> definition.getPath().equals(referencedType))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Expected a reference type"));
    }

    String startsWith = parent.getId() + ".";

    return definitions.stream()
        .filter(definition -> definition.getId().startsWith(startsWith)
            && definition.getId().indexOf('.', startsWith.length()) < 0)
        .collect(Collectors.toList());
  }

  private String elementName(ElementDefinition element) {

    String suffix = element.getPath().substring(element.getPath().lastIndexOf('.') + 1);

    return suffix.endsWith("[x]")
        ? suffix.substring(0, suffix.length() - 3)
        : suffix;
  }

  private StructureDefinition getDefinition(ElementDefinition element) {

    return element.getTypeFirstRep() == null
        || element.getTypeFirstRep().getCode() == null
        || element.getTypeFirstRep().getCode().equals("BackboneElement")
        || element.getTypeFirstRep().getCode().equals("Element")
        ? null
        : (StructureDefinition) validationSupport.fetchStructureDefinition(
            element.getTypeFirstRep().getCode());
  }

  private <T> List<StructureField<T>> singleField(String elementName, T result) {

    return Collections.singletonList(StructureField.property(elementName, result));
  }

  private boolean shouldTerminateRecursive(DefinitionVisitor visitor,
      StructureDefinition structureDefinition,
      Deque<QualifiedPath> stack) {

    ElementDefinition definitionRootElement = structureDefinition.getSnapshot().getElement().get(0);

    return shouldTerminateRecursive(visitor, structureDefinition, definitionRootElement, stack);
  }

  private boolean shouldTerminateRecursive(DefinitionVisitor visitor,
      StructureDefinition rootDefinition,
      ElementDefinition elementDefinition, Deque<QualifiedPath> stack) {


    return shouldTerminateRecursive(visitor,
        new QualifiedPath(rootDefinition.getUrl(), elementDefinition.getPath()),
        stack);
  }

  private boolean shouldTerminateRecursive(DefinitionVisitor visitor,
      QualifiedPath newPath,
      Deque<QualifiedPath> stack) {

    int maxDepth = visitor.getMaxDepth(newPath.getParentTypeUrl(), newPath.getElementPath());

    return stack.stream().filter(path -> path.equals(newPath)).count() > maxDepth;
  }

  private String getFirstProfile(List<CanonicalType> types) {
    if (types == null || types.isEmpty()) {
      return  null;
    }
    if (types.size() > 1) {
      log.warn(String.format("Got %d profiles; expected at most 1!", types.size()));
    }
    return types.get(0).getValue();
  }

  private <T> List<StructureField<T>> extensionElementToFields(DefinitionVisitor<T> visitor,
      StructureDefinition rootDefinition,
      ElementDefinition element,
      List<ElementDefinition> definitions,
      Deque<QualifiedPath> stack) {

    // FIXME: extension is a type rather than an external structure....
    StructureDefinition definition = null;
    // TODO consolidate between `getProfile()` for different FHIR versions.
    String profileUrl = getFirstProfile(element.getTypeFirstRep().getProfile());
    if (profileUrl != null) {
      definition = (StructureDefinition) validationSupport.fetchStructureDefinition(profileUrl);
    }

    List<StructureField<T>> extensions;

    if (definition != null) {

      if (shouldTerminateRecursive(visitor,
          new QualifiedPath(definition.getUrl(), element.getPath()),
          stack)) {

        return Collections.emptyList();

      } else {

        List<ElementDefinition> extensionDefinitions = definition.getSnapshot().getElement();

        ElementDefinition extensionRoot = extensionDefinitions.get(0);

        extensions = visitExtensionDefinition(visitor,
            rootDefinition,
            element.getSliceName(),
            stack,
            definition.getUrl(),
            extensionDefinitions,
            extensionRoot);
      }

    } else {

      if (element.getSliceName() == null) {
        return Collections.emptyList();
      }

      extensions = visitExtensionDefinition(visitor,
          rootDefinition,
          element.getSliceName(),
          stack,
          getFirstProfile(element.getTypeFirstRep().getProfile()),
          definitions,
          element);
    }

    if (!element.getMax().equals("1") && extensions.size() > 0) {
      // the nested extension element has max: *
      return Collections.singletonList(StructureField.extension(
          extensions.get(0).fieldName(),
          extensions.get(0).extensionUrl(),
          extensions.get(0).isModifier(),
          visitor.visitMultiValued(extensions.get(0).fieldName(), extensions.get(0).result())));

    } else {

      return extensions;
    }
  }

  private <T> List<StructureField<T>> visitExtensionDefinition(DefinitionVisitor<T> visitor,
      StructureDefinition rootDefinition,
      String sliceName,
      Deque<QualifiedPath> stack,
      String url,
      List<ElementDefinition> extensionDefinitions,
      ElementDefinition extensionRoot) {

    List<ElementDefinition> children = getChildren(extensionRoot, extensionDefinitions);

    // Extensions may contain either additional extensions or a value field, but not both.

    List<ElementDefinition> childExtensions = children.stream()
        .filter(element -> element.getSliceName() != null)
        .collect(Collectors.toList());

    if (!childExtensions.isEmpty()) {

      List<StructureField<T>> childFields = new ArrayList<>();

      for (ElementDefinition childExtension: childExtensions) {

        List<StructureField<T>> childField = extensionElementToFields(visitor,
            rootDefinition, childExtension, extensionDefinitions, stack);

        childFields.addAll(childField);
      }

      T result = visitor.visitParentExtension(sliceName,
          url,
          childFields);

      if (result == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(
            StructureField.extension(sliceName,
                url,
                extensionRoot.getIsModifier(),
                result));
      }

    } else {

      // The extension has no children, so produce its value.

      Optional<ElementDefinition> valueElement = children.stream()
          .filter(e -> e.getPath().contains("value"))
          .findFirst();

      // FIXME: get the extension URL.
      Optional<ElementDefinition> urlElement = children.stream()
          .filter(e -> e.getPath().endsWith("url"))
          .findFirst();

      String extensionUrl = urlElement.get().getFixed().primitiveValue();

      List<StructureField<T>> childField = elementToFields(visitor, rootDefinition,
          valueElement.get(), extensionDefinitions, stack);

      T result = visitor.visitLeafExtension(sliceName,
          extensionUrl,
          childField.iterator().next().result());

      return Collections.singletonList(
          StructureField.extension(sliceName,
              extensionUrl,
              extensionRoot.getIsModifier(),
              result));

    }
  }

  /**
   * Returns the fields for the given element. The returned stream can be empty
   * (e.g., for elements with max of zero), or have multiple values (for elements
   * that generate fields with additional data in siblings.)
   */
  private <T> List<StructureField<T>> elementToFields(DefinitionVisitor<T> visitor,
      StructureDefinition rootDefinition,
      ElementDefinition element,
      List<ElementDefinition> definitions,
      Deque<QualifiedPath> stack) {

    String elementName = elementName(element);
    log.debug("In elementToFields for element " + element.getPath());

    if (shouldTerminateRecursive(visitor, rootDefinition, element, stack)) {

      return Collections.emptyList();

    } else if (element.getMax().equals("0")) {

      // Fields with max of zero are omitted.
      return Collections.emptyList();

    } else if ("Extension".equals(element.getTypeFirstRep().getCode())) {

      return extensionElementToFields(visitor, rootDefinition, element, definitions, stack);

    } else if (element.getType().size() == 1
        && PRIMITIVE_TYPES.contains(element.getTypeFirstRep().getCode())) {

      T primitiveConverter = visitor
          .visitPrimitive(elementName, element.getTypeFirstRep().getCode());

      if (!element.getMax().equals("1")) {

        return singleField(elementName, visitor.visitMultiValued(elementName, primitiveConverter));
      } else {

        return singleField(elementName, primitiveConverter);
      }

    } else if (element.getPath().endsWith("[x]")) {

      // Use a linked hash map to preserve the order of the fields
      // for iteration.
      Map<String, T> choiceTypes = new LinkedHashMap<>();

      for (TypeRefComponent typeRef: element.getType()) {

        if (PRIMITIVE_TYPES.contains(typeRef.getCode().toLowerCase())) {

          T child = visitor.visitPrimitive(elementName, typeRef.getCode().toLowerCase());
          choiceTypes.put(typeRef.getCode(), child);

        } else {

          StructureDefinition structureDefinition =
              (StructureDefinition) validationSupport
                  .fetchStructureDefinition(typeRef.getCode());

          T child = transform(visitor, element, structureDefinition, new ArrayDeque<>());

          choiceTypes.put(typeRef.getCode(), child);
        }
      }

      StructureField<T> field = new StructureField<>(elementName,
          elementName,
          null,
          false,
          true,
          visitor.visitChoice(elementName, choiceTypes));

      return Collections.singletonList(field);

    } else if (!element.getMax().equals("1")) {

      if (getDefinition(element) != null) {

        // Handle defined data types.
        StructureDefinition definition = getDefinition(element);

        if (shouldTerminateRecursive(visitor, definition, stack)) {

          return Collections.emptyList();

        } else {

          T type = transform(visitor, element, definition, stack);

          return singleField(elementName,
              visitor.visitMultiValued(elementName, type));
        }

      } else {

        List<StructureField<T>> childElements = transformChildren(visitor,
            rootDefinition, definitions, stack, element);

        T result = visitor.visitComposite(elementName,
            element.getPath(),
            elementName,
            rootDefinition.getUrl(),
            childElements);

        List<StructureField<T>> composite = singleField(elementName, result);

        // Array types should produce only a single element.
        if (composite.size() != 1) {
          throw new IllegalStateException("Array type in "
              + element.getPath()
              + " must map to a single structure.");
        }

        // Wrap the item in the corresponding multi-valued type.
        return singleField(elementName,
            visitor.visitMultiValued(elementName, composite.get(0).result()));
      }

    } else if (getDefinition(element) != null) {

      // Handle defined data types.
      StructureDefinition definition = getDefinition(element);

      if (shouldTerminateRecursive(visitor, definition, stack)) {

        return Collections.emptyList();

      } else {
        T type = transform(visitor, element, definition, stack);

        return singleField(elementName(element), type);
      }

    } else {

      // Handle composite type
      List<StructureField<T>> childElements = transformChildren(visitor, rootDefinition,
          definitions, stack, element);

      T result = visitor.visitComposite(elementName,
          element.getPath(),
          elementName,
          rootDefinition.getUrl(),
          childElements);

      return singleField(elementName, result);
    }
  }

  private <T> List<StructureField<T>> transformChildren(DefinitionVisitor<T> visitor,
      StructureDefinition rootDefinition,
      List<ElementDefinition> definitions,
      Deque<QualifiedPath> stack,
      ElementDefinition element) {

    QualifiedPath qualifiedPath = new QualifiedPath(rootDefinition.getUrl(),  element.getPath());

    if (shouldTerminateRecursive(visitor, qualifiedPath, stack)) {

      return Collections.emptyList();

    } else {
      stack.push(qualifiedPath);

      // Handle composite type
      List<StructureField<T>> childElements = new ArrayList<>();

      for (ElementDefinition child: getChildren(element, definitions)) {

        List<StructureField<T>> childFields = elementToFields(visitor, rootDefinition,
            child, definitions, stack);

        childElements.addAll(childFields);
      }

      stack.pop();

      return childElements;
    }
  }

  private <T> StructureField<T> transformContained(DefinitionVisitor<T> visitor,
      StructureDefinition rootDefinition,
      List<StructureDefinition> containedDefinitions,
      Deque<QualifiedPath> stack,
      ElementDefinition element) {

    Map<String, StructureField<T>> containedElements = new LinkedHashMap<>();

    for (StructureDefinition containedDefinition: containedDefinitions) {

      ElementDefinition containedRootElement = containedDefinition.getSnapshot()
          .getElementFirstRep();

      List<ElementDefinition> childDefinitions = containedDefinition.getSnapshot().getElement();

      stack.push(new QualifiedPath(containedDefinition.getUrl(), containedRootElement.getPath()));

      List<StructureField<T>> childElements = transformChildren(visitor,
          containedDefinition,
          childDefinitions,
          stack,
          containedRootElement);

      stack.pop();

      String rootName = elementName(containedRootElement);

      T result = visitor.visitComposite(rootName,
          containedRootElement.getPath(),
          rootName,
          containedDefinition.getUrl(),
          childElements);

      containedElements.put(rootName, StructureField.property(rootName, result));
    }

    T result = visitor.visitContained(element.getPath() + ".contained",
        rootDefinition.getUrl(),
        containedElements);

    return StructureField.property("contained", result);
  }

  @Override
  public FhirConversionSupport conversionSupport() {

    return CONVERSION_SUPPORT;
  }

  @Override
  public <T> T transform(DefinitionVisitor<T> visitor, String resourceTypeUrl) {

    StructureDefinition definition = (StructureDefinition) context.getValidationSupport()
        .fetchStructureDefinition(resourceTypeUrl);

    if (definition == null) {

      throw new IllegalArgumentException("Unable to find definition for " + resourceTypeUrl);
    }

    return transform(visitor, definition);
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @return The schema as a Spark StructType
   */
  public <T> T transform(DefinitionVisitor<T> visitor, StructureDefinition definition) {

    return transform(visitor, null, definition, new ArrayDeque<>());
  }

  @Override
  public <T> T transform(DefinitionVisitor<T> visitor,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls) {

    StructureDefinition definition = (StructureDefinition) context.getValidationSupport()
        .fetchStructureDefinition(resourceTypeUrl);

    if (definition == null) {

      throw new IllegalArgumentException("Unable to find definition for " + resourceTypeUrl);
    }

    List<StructureDefinition> containedDefinitions = containedResourceTypeUrls.stream()
        .map(containedResourceTypeUrl -> {
          StructureDefinition containedDefinition = (StructureDefinition) context
              .getValidationSupport()
              .fetchStructureDefinition(containedResourceTypeUrl);

          if (containedDefinition == null) {

            throw new IllegalArgumentException("Unable to find definition for "
                + containedResourceTypeUrl);
          }

          return containedDefinition;
        })
        .collect(Collectors.toList());

    return transformRoot(visitor, definition, containedDefinitions, new ArrayDeque<>());
  }

  /**
   * Transforms the given FHIR structure definition.
   *
   * @param visitor the visitor performing the transformation
   * @param parentElement the element containing this definition for additional type information,
   *     or null if it is not contained in a parent element.
   * @param definition the FHIR structure definition to be converted
   * @param stack a stack of FHIR type URLs to detect recursive definitions.
   *
   * @return the transformed structure, or null if it should not be included in the parent.
   */
  private <T> T transform(DefinitionVisitor<T> visitor,
      ElementDefinition parentElement,
      StructureDefinition definition,
      Deque<QualifiedPath> stack) {

    ElementDefinition definitionRootElement = definition.getSnapshot().getElement().get(0);

    List<ElementDefinition> definitions = definition.getSnapshot().getElement();

    ElementDefinition root = definitions.get(0);

    stack.push(new QualifiedPath(definition.getUrl(), definitionRootElement.getPath()));

    List<StructureField<T>> childElements = transformChildren(visitor, definition,
        definitions, stack, root);

    stack.pop();

    if ("Reference".equals(definition.getType())) {

      // TODO: if this is in an option there may be other non-reference types here?
      String rootName = elementName(root);

      List<String> referenceTypes = parentElement.getType()
          .stream()
          .filter(type -> "Reference".equals(type.getCode()))
          .filter(type -> type.getTargetProfile() != null)
          .map(type -> {

            IValidationSupport validation = context.getValidationSupport();

            StructureDefinition targetDefinition = (StructureDefinition)
                validation.fetchStructureDefinition(getFirstProfile(type.getTargetProfile()));

            return targetDefinition.getType();
          })
          .sorted()
          .collect(Collectors.toList());

      return visitor.visitReference(rootName, referenceTypes, childElements);

    } else {

      String rootName = elementName(root);

      return visitor.visitComposite(rootName,
          rootName,
          rootName,
          definition.getUrl(),
          childElements);
    }
  }

  private <T> T transformRoot(DefinitionVisitor<T> visitor,
      StructureDefinition definition,
      List<StructureDefinition> containedDefinitions,
      Deque<QualifiedPath> stack) {

    ElementDefinition definitionRootElement = definition.getSnapshot().getElementFirstRep();

    List<ElementDefinition> definitions = definition.getSnapshot().getElement();

    ElementDefinition root = definitions.get(0);

    stack.push(new QualifiedPath(definition.getUrl(), definitionRootElement.getPath()));

    List<StructureField<T>> childElements = transformChildren(visitor,
        definition,
        definitions,
        stack,
        root);

    // If there are contained definitions, create a Resource Container StructureField
    if (containedDefinitions.size() > 0) {

      StructureField<T> containedElement = transformContained(visitor,
          definition,
          containedDefinitions,
          stack,
          root);

      // Replace default StructureField with constructed Resource Container StructureField
      childElements.set(5, containedElement);
    }

    stack.pop();

    String rootName = elementName(root);

    return visitor.visitComposite(rootName,
        rootName,
        rootName,
        definition.getUrl(),
        childElements);
  }
}
