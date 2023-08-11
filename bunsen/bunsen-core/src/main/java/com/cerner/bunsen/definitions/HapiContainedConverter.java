package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.HapiCompositeConverter.CompositeFieldSetter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Partial converter implementation for contained structures. Contained converters are distinct from
 * {@link HapiCompositeConverter} in that a Contained converter must translate between a static
 * Resource Container and FHIR's Any-typed container list of FHIR resources.
 *
 * @param <T> the type of the schema produced by the converter.
 */
public abstract class HapiContainedConverter<T> extends HapiConverter<T> {

  private final Map<String, StructureField<HapiConverter<T>>> contained;

  private final T structType;

  /**
   * Retrieves the contained elements from their container, associated with their type.
   *
   * @param container the Resource Container
   * @return the {@link List} of {@link ContainerEntry}.
   */
  protected abstract List<ContainerEntry> getContained(Object container);

  /**
   * Constructs a statically typed Resource Container populated with all contained entries.
   *
   * @param contained the entries to be contained.
   * @return the Resource Container.
   */
  protected abstract Object createContained(Object[] contained);

  /** Represents the association of a contained element to its type. */
  protected final class ContainerEntry {

    private final String elementType;
    private final Object element;

    public ContainerEntry(String elementType, Object element) {

      this.elementType = elementType;
      this.element = element;
    }

    public String getElementType() {

      return elementType;
    }

    public Object getElement() {

      return element;
    }
  }

  private final class ContainedFieldSetter implements HapiFieldSetter {

    private final Map<String, CompositeFieldSetter> contained;

    private ContainedFieldSetter(Map<String, CompositeFieldSetter> contained) {

      this.contained = contained;
    }

    @Override
    public void setField(IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object object) {

      List<ContainerEntry> containedEntries = getContained(object);

      for (ContainerEntry containedEntry : containedEntries) {

        String containedElementType = containedEntry.getElementType();
        IBase resource = contained.get(containedElementType).toHapi(containedEntry.getElement());

        fieldToSet.getMutator().addValue(parentObject, resource);
      }
    }
  }

  protected HapiContainedConverter(
      Map<String, StructureField<HapiConverter<T>>> contained, T structType) {

    this.contained = contained;
    this.structType = structType;
  }

  @Override
  public Object fromHapi(Object input) {

    List containedList = (List) input;

    Object[] values = new Object[containedList.size()];

    for (int valueIndex = 0; valueIndex < containedList.size(); ++valueIndex) {

      IBase composite = (IBase) containedList.get(valueIndex);
      StructureField<HapiConverter<T>> schemaEntry = contained.get(composite.fhirType());

      values[valueIndex] = schemaEntry.result().fromHapi(composite);
    }

    return createContained(values);
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    Map<String, CompositeFieldSetter> fieldSetters = new LinkedHashMap<>();

    for (Entry<String, StructureField<HapiConverter<T>>> containedConverter :
        contained.entrySet()) {

      for (BaseRuntimeElementDefinition elementDefinition : elementDefinitions) {

        if (elementDefinition.getName().equals(containedConverter.getKey())) {

          fieldSetters.put(
              containedConverter.getKey(),
              (CompositeFieldSetter)
                  containedConverter.getValue().result().toHapiConverter(elementDefinition));
        }
      }
    }

    return new ContainedFieldSetter(fieldSetters);
  }

  @Override
  public T getDataType() {

    return structType;
  }
}
