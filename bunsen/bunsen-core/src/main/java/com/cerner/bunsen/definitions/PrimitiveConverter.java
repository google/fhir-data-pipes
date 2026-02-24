package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IPrimitiveType;
import org.jspecify.annotations.Nullable;

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

  @Nullable
  protected Object fromHapi(IPrimitiveType primitive) {
    return primitive.getValue();
  }

  @Nullable
  @Override
  public Object fromHapi(Object input) {
    // TODO: remove this hack! It is added to address this bug:
    // https://github.com/google/fhir-data-pipes/issues/1014
    // with root cause being:
    // https://github.com/hapifhir/org.hl7.fhir.core/issues/1800
    if (!(input instanceof IPrimitiveType)) {
      return "";
    }
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

  @Override
  public HapiConverter<T> merge(HapiConverter<T> other) throws ProfileException {
    HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
    validateIfElementTypesAreSame(other);
    return this;
  }

  private void validateIfElementTypesAreSame(HapiConverter other) throws ProfileException {
    Preconditions.checkNotNull(other, "The other HapiConverter cannot be null");
    if (!(other instanceof PrimitiveConverter
        && Objects.equals(this.elementType, other.getElementType()))) {
      throw new ProfileException(
          String.format(
              "Cannot merge %s FHIR Type %s with %s FHIR Type %s",
              this.getElementType(),
              this.getClass().getName(),
              other.getElementType(),
              other.getClass().getName()));
    }
  }
}
