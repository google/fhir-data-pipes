package com.cerner.bunsen.definitions;

import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class EnumConverter<T> extends StringConverter<T> {

  public EnumConverter(T dataType) {

    super(dataType);
  }

  @Override
  public void toHapi(Object input, IPrimitiveType primitive) {

    if ("?".equals(input)) {

      input = null;
    }

    primitive.setValueAsString((String) input);
  }

  @Override
  protected Object fromHapi(IPrimitiveType primitive) {

    return "?".equals(primitive.getValueAsString()) ? null : primitive.getValueAsString();
  }
}
