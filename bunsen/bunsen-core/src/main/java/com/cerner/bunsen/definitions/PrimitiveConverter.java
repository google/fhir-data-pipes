package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

public abstract class PrimitiveConverter<T> extends HapiConverter<T> {

  private class PrimitiveFieldSetter implements HapiFieldSetter, HapiObjectConverter {

    private final BaseRuntimeElementDefinition elementDefinition;

    PrimitiveFieldSetter(BaseRuntimeElementDefinition elementDefinition) {
      this.elementDefinition = elementDefinition;
    }

    @Override
    public void setField(IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object object) {

      IPrimitiveType element =
          (IPrimitiveType)
              elementDefinition.newInstance(fieldToSet.getInstanceConstructorArguments());

      PrimitiveConverter.this.toHapi(object, element);

      fieldToSet.getMutator().setValue(parentObject, element);
    }

    @Override
    public IBase toHapi(Object input) {

      IPrimitiveType element = (IPrimitiveType) elementDefinition.newInstance();

      PrimitiveConverter.this.toHapi(input, element);

      return element;
    }
  }

  private final String elementType;

  public PrimitiveConverter(String elementType) {
    this.elementType = elementType;
  }

  /**
   * Helper method that will set the HAPI value.
   *
   * @param input the input object
   * @param primitive the FHIR primitive to set
   */
  public void toHapi(Object input, IPrimitiveType primitive) {
    primitive.setValue(input);
  }

  protected Object fromHapi(IPrimitiveType primitive) {
    return primitive.getValue();
  }

  @Override
  public Object fromHapi(Object input) {

    return fromHapi((IPrimitiveType) input);
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {
    return new PrimitiveFieldSetter(elementDefinitions[0]);
  }

  @Override
  public String getElementType() {
    return elementType;
  }
}
