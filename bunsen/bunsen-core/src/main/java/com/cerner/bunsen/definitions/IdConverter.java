package com.cerner.bunsen.definitions;

import com.google.common.base.Preconditions;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class IdConverter<T> extends StringConverter<T> {

  public IdConverter(T dataType) {
    super(dataType);
  }

  @Override
  protected Object fromHapi(IPrimitiveType primitive) {
    // We do this hack to work around the issue in R4 where `id`s are of type `System.String`
    // instead of just type `id`. Note `id` elements of FHIR types are strings and not `IIdType`!
    if (!(primitive instanceof IIdType)) {
      return super.fromHapi(primitive);
    }
    return ((IIdType) primitive).getIdPart();
  }
}
