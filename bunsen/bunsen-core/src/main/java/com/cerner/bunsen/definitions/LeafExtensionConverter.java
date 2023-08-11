package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseDatatype;
import org.hl7.fhir.instance.model.api.IBaseExtension;

/**
 * Converter implementation for extension leaf primitives.
 *
 * @param <T> the type of the schema produced by the converter.
 */
public class LeafExtensionConverter<T> extends HapiConverter<T> {

  class LeafExensionFieldSetter implements HapiFieldSetter, HapiObjectConverter {

    private final HapiObjectConverter valuetoHapiConverter;

    private final BaseRuntimeElementCompositeDefinition elementDefinition;

    LeafExensionFieldSetter(
        BaseRuntimeElementCompositeDefinition elementDefinition,
        HapiObjectConverter valuetoHapiConverter) {

      this.elementDefinition = elementDefinition;
      this.valuetoHapiConverter = valuetoHapiConverter;
    }

    @Override
    public void setField(
        IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object sparkObject) {

      IBase hapiObject = valuetoHapiConverter.toHapi(sparkObject);

      IBaseExtension extension = (IBaseExtension) elementDefinition.newInstance(extensionUrl);

      extension.setValue((IBaseDatatype) hapiObject);

      fieldToSet.getMutator().addValue(parentObject, extension);
    }

    /**
     * Converts an object from a different data model to a HAPI object.
     *
     * @param input the object to convert
     * @return the HAPI equivalent.
     */
    @Override
    public IBase toHapi(Object input) {

      IBase hapiObject = valuetoHapiConverter.toHapi(input);

      IBaseExtension extension = (IBaseExtension) elementDefinition.newInstance(extensionUrl);

      extension.setValue((IBaseDatatype) hapiObject);

      return extension;
    }
  }

  private final String extensionUrl;

  private final HapiConverter<T> valueConverter;

  /**
   * Constructs a converter for the leaf extension.
   *
   * @param extensionUrl the URL of the extension
   * @param valueConverter the converter for the extension's value.
   */
  public LeafExtensionConverter(String extensionUrl, HapiConverter valueConverter) {
    this.extensionUrl = extensionUrl;
    this.valueConverter = valueConverter;
  }

  @Override
  public Object fromHapi(Object input) {

    IBaseExtension extension = (IBaseExtension) input;

    return valueConverter.fromHapi(extension.getValue());
  }

  @Override
  public T getDataType() {
    return valueConverter.getDataType();
  }

  @Override
  public String extensionUrl() {
    return extensionUrl;
  }

  private BaseRuntimeElementDefinition fetchElementDefinitionForField(
      String valueField, RuntimeCompositeDatatypeDefinition definition) {
    BaseRuntimeElementDefinition valueDefinition =
        definition.getChildByName(valueField).getChildByName(valueField);
    return valueDefinition;
  }

  @Override
  public HapiFieldSetter toHapiConverter(BaseRuntimeElementDefinition... elementDefinitions) {

    RuntimeCompositeDatatypeDefinition definition =
        (RuntimeCompositeDatatypeDefinition) elementDefinitions[0];

    if (valueConverter instanceof HapiChoiceConverter) {
      // NOTE: When valueConverter is HapiChoiceToAvroConverter, getElementType() is null and the
      // code after this block will fail. In this case we need to iterate over all possible types
      // which is not currently supported. Note `HapiChoiceConverter.toHapiConverter` return value
      // is _not_ a HapiObjectConverter which is needed for creating a `LeafExensionFieldSetter`.
      // So the fix is not trivial as the following cast fails.
      // sparkToHapi = (HapiObjectConverter) valueConverter.toHapiConverter(choiceElementDefs);
      throw new IllegalStateException("Union types are not yet supported for extensions.");
    }
    // Get the structure definition(s) of the value(s).
    //
    // TODO: The assumptions being made here is wrong, in particular it is broken for the R4
    // extension StructureDefinitions, where the `value` element `path` ends in `value[x]`.
    // To fix this we currently have a hack in `R4StructureDefinitions.elementToFields` to force
    // a single type for extensions.
    // https://github.com/google/fhir-data-pipes/issues/559
    String fieldName = "value" + valueConverter.getElementType();
    BaseRuntimeElementDefinition valueDefinition =
        fetchElementDefinitionForField(fieldName, definition);
    HapiObjectConverter sparkToHapi =
        (HapiObjectConverter) valueConverter.toHapiConverter(valueDefinition);
    return new LeafExensionFieldSetter(definition, sparkToHapi);
  }
}
