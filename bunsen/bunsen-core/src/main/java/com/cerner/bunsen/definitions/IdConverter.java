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
    Preconditions.checkArgument(primitive instanceof IIdType);
    return ((IIdType) primitive).getIdPart();
  }
}
