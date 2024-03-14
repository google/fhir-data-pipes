package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.HapiMergeException;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class IdConverter<T> extends StringConverter<T> {

  public IdConverter(T dataType) {
    super(dataType);
  }

  @Override
  protected Object fromHapi(IPrimitiveType primitive) {
    // We do this hack to work around the issue that `id` has type `System.String` in R4 (not `id`).
    // Note `id` elements of FHIR _types_ (not resources) are strings, not `IIdType`!
    if (primitive instanceof IIdType) {
      return ((IIdType) primitive).getIdPart();
    }
    return super.fromHapi(primitive);
  }

  @Override
  public HapiConverter merge(HapiConverter other) throws HapiMergeException {
    if (other != null && other instanceof IdConverter && "String".equals(other.getElementType())) {
      return this;
    }
    throw new HapiMergeException(
        String.format(
            "Cannot merge String Fhir Type IdConverter with %s ",
            other != null
                ? String.format(
                    "%s Fhir Type %s", other.getElementType(), other.getClass().getName())
                : null));
  }
}
