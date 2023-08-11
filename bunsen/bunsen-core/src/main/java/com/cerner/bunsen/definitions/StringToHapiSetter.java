package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.HapiConverter.HapiFieldSetter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class StringToHapiSetter implements HapiFieldSetter, HapiObjectConverter {

  private final BaseRuntimeElementDefinition elementDefinition;

  StringToHapiSetter(BaseRuntimeElementDefinition elementDefinition) {
    this.elementDefinition = elementDefinition;
  }

  @Override
  public void setField(
      IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object sparkObject) {
    fieldToSet.getMutator().setValue(parentObject, toHapi(sparkObject));
  }

  @Override
  public IBase toHapi(Object sparkObject) {

    IPrimitiveType element = (IPrimitiveType) elementDefinition.newInstance();

    element.setValueAsString((String) sparkObject);

    return element;
  }
}
