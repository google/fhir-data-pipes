package com.cerner.bunsen.avro.converters;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverterUtil;
import com.cerner.bunsen.exception.ProfileException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.hl7.fhir.instance.model.api.IBase;

public class NoOpConverter extends HapiConverter<Schema> {

  @Override
  public Object fromHapi(Object input) {
    return null;
  }

  @Override
  public Schema getDataType() {
    return Schema.create(Type.STRING);
  }

  private static class FieldSetter implements HapiFieldSetter, HapiObjectConverter {

    @Override
    public void setField(IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object value) {}

    @Override
    public IBase toHapi(Object input) {
      return null;
    }
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    return new FieldSetter();
  }

  @Override
  public HapiConverter merge(HapiConverter other) throws ProfileException {
    HapiConverterUtil.validateIfConvertersCanBeMerged(this, other);
    return this;
  }

  public static final NoOpConverter INSTANCE = new NoOpConverter();
}
