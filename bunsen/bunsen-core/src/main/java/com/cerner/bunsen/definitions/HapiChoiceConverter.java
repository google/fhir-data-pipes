package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.hl7.fhir.instance.model.api.IBase;

public abstract class HapiChoiceConverter<T> extends HapiConverter<T> {

  protected abstract Object getChild(Object composite, int index);

  protected abstract Object createComposite(Object[] children);

  private final Map<String, HapiConverter<T>> choiceTypes;

  private final T structType;

  private final FhirConversionSupport fhirSupport;

  private final class ChoiceFieldSetter implements HapiFieldSetter {

    private final Map<String, HapiFieldSetter> choiceFieldSetters;

    ChoiceFieldSetter(Map<String, HapiFieldSetter> choiceFieldSetters) {
      this.choiceFieldSetters = choiceFieldSetters;
    }

    @Override
    public void setField(
        IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object composite) {

      Iterator<Entry<String, HapiFieldSetter>> setterIterator =
          choiceFieldSetters.entrySet().iterator();

      // Co-iterate with an index so we place the correct values into the corresponding locations.
      for (int valueIndex = 0; valueIndex < choiceTypes.size(); ++valueIndex) {

        Map.Entry<String, HapiFieldSetter> setterEntry = setterIterator.next();

        if (getChild(composite, valueIndex) != null) {

          HapiFieldSetter setter = setterEntry.getValue();

          setter.setField(parentObject, fieldToSet, getChild(composite, valueIndex));

          // We set a non-null field for the choice type, so stop looking.
          break;
        }
      }
    }
  }

  protected HapiChoiceConverter(
      Map<String, HapiConverter<T>> choiceTypes, T structType, FhirConversionSupport fhirSupport) {

    this.choiceTypes = choiceTypes;
    this.structType = structType;
    this.fhirSupport = fhirSupport;
  }

  @Override
  public Object fromHapi(Object input) {

    String fhirType = fhirSupport.fhirType((IBase) input);

    Object[] values = new Object[choiceTypes.size()];

    Iterator<Map.Entry<String, HapiConverter<T>>> schemaIterator =
        choiceTypes.entrySet().iterator();

    // Co-iterate with an index so we place the correct values into the corresponding locations.
    for (int valueIndex = 0; valueIndex < choiceTypes.size(); ++valueIndex) {

      Map.Entry<String, HapiConverter<T>> choiceEntry = schemaIterator.next();

      // Set the nested field that matches the choice type.
      if (choiceEntry.getKey().equals(fhirType)) {

        HapiConverter converter = choiceEntry.getValue();

        values[valueIndex] = converter.fromHapi(input);
      }
    }

    return createComposite(values);
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    Map<String, HapiFieldSetter> fieldSetters = new LinkedHashMap<>();

    for (Map.Entry<String, HapiConverter<T>> choiceEntry : choiceTypes.entrySet()) {

      // The list is small and only consumed when generating the conversion functions,
      // so a nested loop isn't a performance issue.
      for (BaseRuntimeElementDefinition elementDefinition : elementDefinitions) {

        if (elementDefinition.getName().equals(choiceEntry.getKey())) {

          fieldSetters.put(
              choiceEntry.getKey(), choiceEntry.getValue().toHapiConverter(elementDefinition));
        }
      }
    }

    return new ChoiceFieldSetter(fieldSetters);
  }

  @Override
  public T getDataType() {
    return structType;
  }
}
