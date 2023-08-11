package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeElemContainedResourceList;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.instance.model.api.IAnyResource;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseExtension;
import org.hl7.fhir.instance.model.api.IBaseHasExtensions;
import org.hl7.fhir.instance.model.api.IBaseHasModifierExtensions;

/**
 * Partial converter implementation for composite structures.
 *
 * @param <T> the type of the schema produced by the converter.
 */
public abstract class HapiCompositeConverter<T> extends HapiConverter<T> {

  private final String elementType;

  private final List<StructureField<HapiConverter<T>>> children;

  protected final T structType;

  private final String extensionUrl;

  private final FhirConversionSupport fhirSupport;

  protected abstract Object getChild(Object composite, int index);

  protected abstract Object createComposite(Object[] children);

  protected abstract boolean isMultiValued(T schemaType);

  /** Field setter that does nothing for synthetic or unsupported field types. */
  private static final class NoOpFieldSetter implements HapiFieldSetter, HapiObjectConverter {

    @Override
    public void setField(
        IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object sourceObject) {}

    @Override
    public IBase toHapi(Object input) {
      return null;
    }
  }

  private static final HapiFieldSetter NOOP_FIELD_SETTER = new NoOpFieldSetter();

  protected final class CompositeFieldSetter implements HapiFieldSetter, HapiObjectConverter {

    private final List<StructureField<HapiFieldSetter>> children;

    private final BaseRuntimeElementCompositeDefinition compositeDefinition;

    CompositeFieldSetter(
        BaseRuntimeElementCompositeDefinition compositeDefinition,
        List<StructureField<HapiFieldSetter>> children) {

      this.compositeDefinition = compositeDefinition;
      this.children = children;
    }

    @Override
    public IBase toHapi(Object sourceObject) {

      IBase fhirObject = compositeDefinition.newInstance();

      Iterator<StructureField<HapiFieldSetter>> childIterator = children.iterator();

      for (int fieldIndex = 0; fieldIndex < children.size(); ++fieldIndex) {

        StructureField<HapiFieldSetter> child = childIterator.next();

        // Some children are ignored, for instance when terminating recursive fields.
        if (child == null || child.result() == null) {
          continue;
        }

        Object fieldValue = getChild(sourceObject, fieldIndex);

        if (fieldValue != null) {

          if (child.extensionUrl() != null) {

            BaseRuntimeChildDefinition childDefinition =
                child.isModifier()
                    ? compositeDefinition.getChildByName("modifierExtension")
                    : compositeDefinition.getChildByName("extension");

            child.result().setField(fhirObject, childDefinition, fieldValue);

          } else {

            String propertyName =
                child.isChoice() ? child.propertyName() + "[x]" : child.propertyName();

            BaseRuntimeChildDefinition childDefinition =
                compositeDefinition.getChildByName(propertyName);

            child.result().setField(fhirObject, childDefinition, fieldValue);
          }
        }
      }

      if (extensionUrl != null) {

        ((IBaseExtension) fhirObject).setUrl(extensionUrl);
      }

      return fhirObject;
    }

    @Override
    public void setField(
        IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object sourceObject) {

      IBase fhirObject = toHapi(sourceObject);

      if (extensionUrl != null) {

        fieldToSet.getMutator().addValue(parentObject, fhirObject);

      } else {
        fieldToSet.getMutator().setValue(parentObject, fhirObject);
      }
    }
  }

  protected HapiCompositeConverter(
      String elementType,
      List<StructureField<HapiConverter<T>>> children,
      T structType,
      FhirConversionSupport fhirSupport,
      String extensionUrl) {
    // A composite type should have at least one child.
    Preconditions.checkArgument(!children.isEmpty());
    this.elementType = elementType;
    this.children = children;
    this.structType = structType;
    this.extensionUrl = extensionUrl;
    this.fhirSupport = fhirSupport;
  }

  @Override
  public Object fromHapi(Object input) {

    IBase composite = (IBase) input;

    Object[] values = new Object[children.size()];

    int valueIndex = 0;
    Iterator<StructureField<HapiConverter<T>>> schemaIterator = children.iterator();

    if (composite instanceof IAnyResource) {

      StructureField<HapiConverter<T>> schemaEntry = schemaIterator.next();
      // We do not want id in nested elements or sub-elements.
      // https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#id-fields-omitted
      // So, there can be schema elements where id is not present in which case we should
      // check whether field name is id; if yes, proceed accordingly and set the iterator
      // for meta element and if not then the field is going to be meta element.
      if (schemaEntry.fieldName().equals("id")) {
        // Id element.
        values[0] = schemaEntry.result().fromHapi(((IAnyResource) composite).getIdElement());
        valueIndex++;

        schemaEntry = schemaIterator.next();
      }
      // Meta element.
      values[valueIndex++] = schemaEntry.result().fromHapi(((IAnyResource) composite).getMeta());
    }

    Map<String, List> properties = fhirSupport.compositeValues(composite);

    // Co-iterate with an index so we place the correct values into the corresponding locations.
    for (; valueIndex < children.size(); ++valueIndex) {

      StructureField<HapiConverter<T>> schemaEntry = schemaIterator.next();

      String propertyName = schemaEntry.propertyName();

      // Append the [x] suffix for choice properties.
      if (schemaEntry.isChoice()) {
        propertyName = propertyName + "[x]";
      }

      HapiConverter<T> converter = schemaEntry.result();

      List propertyValues = properties.get(propertyName);

      if (propertyValues != null && !propertyValues.isEmpty()) {

        if (isMultiValued(converter.getDataType())) {

          values[valueIndex] = schemaEntry.result().fromHapi(propertyValues);

        } else {

          values[valueIndex] = schemaEntry.result().fromHapi(propertyValues.get(0));
        }
      } else if (converter.extensionUrl() != null) {

        // No corresponding property for the name, so see if it is an Extension or ModifierExtention
        List<? extends IBaseExtension> extensions =
            schemaEntry.isModifier()
                ? ((IBaseHasModifierExtensions) composite).getModifierExtension()
                : ((IBaseHasExtensions) composite).getExtension();

        for (IBaseExtension extension : extensions) {

          if (extension.getUrl().equals(converter.extensionUrl())) {

            values[valueIndex] = schemaEntry.result().fromHapi(extension);
          }
        }

      } else if (converter instanceof MultiValueConverter
          && ((MultiValueConverter) converter).getElementConverter().extensionUrl() != null) {

        final String extensionUrl =
            ((MultiValueConverter) converter).getElementConverter().extensionUrl();

        List<? extends IBaseExtension> extensions =
            schemaEntry.isModifier()
                ? ((IBaseHasModifierExtensions) composite).getModifierExtension()
                : ((IBaseHasExtensions) composite).getExtension();

        final List<? extends IBaseExtension> extensionList =
            extensions.stream()
                .filter(extension -> extension.getUrl().equals(extensionUrl))
                .collect(Collectors.toList());

        if (extensionList.size() > 0) {
          values[valueIndex] = schemaEntry.result().fromHapi(extensionList);
        }
      }
    }

    return createComposite(values);
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    BaseRuntimeElementDefinition elementDefinition = elementDefinitions[0];

    // The Contained element is not set when discovered recursively as a child, but is rather set
    // explicitly from the root
    if (elementDefinition instanceof RuntimeElemContainedResourceList) {
      return NOOP_FIELD_SETTER;
    }

    if (!(elementDefinition instanceof BaseRuntimeElementCompositeDefinition)) {
      throw new IllegalArgumentException(
          "Composite converter must be given a "
              + "single composite element, received: "
              + elementDefinition.getName());
    }

    BaseRuntimeElementCompositeDefinition compositeDefinition =
        (BaseRuntimeElementCompositeDefinition) elementDefinition;

    List<StructureField<HapiFieldSetter>> toHapiChildren =
        children.stream()
            .map(
                child -> {
                  HapiFieldSetter childConverter;

                  if ("contained".equals(child.propertyName())) {

                    // Handle contained resources.
                    HapiFieldSetter containedFieldSetter = NOOP_FIELD_SETTER;

                    if (elementDefinitions.length > 1) {

                      BaseRuntimeElementDefinition containedDefinition =
                          compositeDefinition
                              .getChildByName("contained")
                              .getChildByName("contained");

                      BaseRuntimeElementDefinition[] containedDefinitions =
                          new BaseRuntimeElementDefinition[elementDefinitions.length];

                      containedDefinitions[0] = containedDefinition;

                      System.arraycopy(
                          elementDefinitions,
                          1,
                          containedDefinitions,
                          1,
                          containedDefinitions.length - 1);

                      containedFieldSetter = child.result().toHapiConverter(containedDefinitions);
                    }

                    return new StructureField<>(
                        "contained", "contained", null, false, false, containedFieldSetter);

                  } else if (child.extensionUrl() != null) {

                    // Handle extensions.
                    BaseRuntimeChildDefinition childDefinition =
                        compositeDefinition.getChildByName("extension");

                    childConverter =
                        child.result().toHapiConverter(childDefinition.getChildByName("extension"));

                  } else {

                    String propertyName = child.propertyName();

                    // Append the [x] suffix for choice properties.
                    if (child.isChoice()) {

                      propertyName = propertyName + "[x]";
                    }

                    BaseRuntimeChildDefinition childDefinition =
                        compositeDefinition.getChildByName(propertyName);

                    BaseRuntimeElementDefinition[] childElementDefinitions;

                    if (child.isChoice()) {

                      int childCount = childDefinition.getValidChildNames().size();

                      childElementDefinitions = new BaseRuntimeElementDefinition[childCount];

                      int index = 0;

                      for (String childName : childDefinition.getValidChildNames()) {

                        childDefinition.getChildByName(childName);

                        childElementDefinitions[index++] =
                            childDefinition.getChildByName(childName);
                      }

                    } else {

                      childElementDefinitions =
                          new BaseRuntimeElementDefinition[] {
                            childDefinition.getChildByName(propertyName)
                          };
                    }

                    childConverter = child.result().toHapiConverter(childElementDefinitions);
                  }

                  return new StructureField<>(
                      child.propertyName(),
                      child.fieldName(),
                      child.extensionUrl(),
                      child.isModifier(),
                      child.isChoice(),
                      childConverter);
                })
            .collect(Collectors.toList());

    return new CompositeFieldSetter(compositeDefinition, toHapiChildren);
  }

  @Override
  public T getDataType() {
    return structType;
  }

  @Override
  public String extensionUrl() {
    return extensionUrl;
  }

  @Override
  public String getElementType() {
    return elementType;
  }
}
