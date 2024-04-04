package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.ProfileException;
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
  public HapiConverter merge(HapiConverter other) throws ProfileException {
    HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
    validateIfElementTypesAreSame(other);
    return this;
  }
}
