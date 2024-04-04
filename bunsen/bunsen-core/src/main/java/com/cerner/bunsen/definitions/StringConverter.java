package com.cerner.bunsen.definitions;

import com.cerner.bunsen.exception.ProfileException;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

public class StringConverter<T> extends PrimitiveConverter<T> {

  private final T dataType;

  public StringConverter(T dataType) {
    super("String");
    this.dataType = dataType;
  }

  @Override
  public void toHapi(Object input, IPrimitiveType primitive) {
    // Note we cannot simply cast `input` to `String` because the type used
    // is not necessarily String when reading the Avro record with a `string`
    // field; it can be Utf8, and it is controlled by `avro.java.string` field
    // type property in the schema, see:
    // https://github.com/apache/parquet-mr/commit/918609f2cc4e4de95445ce4fdd7dc952b9625017
    // https://github.com/apache/parquet-mr/blob/8264d8b2329f6e7a9ad900e2f9d32abee80f29ff/parquet-avro/src/main/java/org/apache/parquet/avro/AvroRecordConverter.java#L410
    primitive.setValueAsString(input.toString());
  }

  @Override
  protected Object fromHapi(IPrimitiveType primitive) {
    return primitive.getValueAsString();
  }

  @Override
  public T getDataType() {

    return dataType;
  }

  @Override
  public HapiConverter merge(HapiConverter other) throws ProfileException {
    HapiConverterUtil.validateIfImplementationClassesAreSame(this, other);
    validateIfElementTypesAreSame(other);
    return this;
  }
}
