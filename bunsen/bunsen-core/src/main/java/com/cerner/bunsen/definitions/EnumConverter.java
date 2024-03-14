package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.HapiMergeException;
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

  @Override
  public HapiConverter merge(HapiConverter other) throws HapiMergeException {
    if (other != null
        && other instanceof EnumConverter
        && "String".equals(other.getElementType())) {
      return this;
    }
    throw new HapiMergeException(
        String.format(
            "Cannot merge String Fhir Type EnumConverter with %s ",
            other != null
                ? String.format(
                    "%s Fhir Type %s", other.getElementType(), other.getClass().getName())
                : null));
  }
}
