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

    // We can't assume the type is necessarily `String` for Avro `string`
    // types; it can be Utf8 too, see:
    // https://github.com/apache/parquet-mr/commit/918609f2cc4e4de95445ce4fdd7dc952b9625017
    primitive.setValueAsString(input.toString());
  }

  @Override
  protected Object fromHapi(IPrimitiveType primitive) {

    return "?".equals(primitive.getValueAsString()) ? null : primitive.getValueAsString();
  }
}
