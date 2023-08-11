package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Base class for converting HAPI resources to an alternative object model, such as Spark rows or
 * Avro objects.
 */
public abstract class HapiConverter<T> {

  /** Supporting interface to convert and set a field on a HAPI object. */
  public interface HapiFieldSetter {

    /**
     * Sets the value to the corresponding field on a FHIR object.
     *
     * @param parentObject the composite object getting its field set
     * @param fieldToSet the runtime definition of the field to set.
     * @param value the value to be converted and set on the FHIR object.
     */
    void setField(IBase parentObject, BaseRuntimeChildDefinition fieldToSet, Object value);
  }

  /**
   * Supporting interface to convert an object from a different data model to a HAPI object. This
   * interface is separated because not all HAPI objects can be created with a simple function. In
   * some cases additional context is needed, such as the HAPI parent object which defines the
   * enumerated values in a given field. This is why this interface is separate from the {@link
   * HapiFieldSetter} interface.
   */
  public interface HapiObjectConverter {

    /**
     * Converts an object from a different data model to a HAPI object.
     *
     * @param input the object to convert
     * @return the HAPI equivalent.
     */
    IBase toHapi(Object input);
  }

  /**
   * Supporting interface to convert multiple values for the corresponding field on a FHIR object.
   */
  public interface MultiValueConverter {

    HapiConverter getElementConverter();
  }

  /**
   * Converts a HAPI object or list of objects to the equivalent in the alternative data model.
   *
   * @param input a HAPI object.
   * @return the data model-specific equivalent.
   */
  public abstract Object fromHapi(Object input);

  /**
   * Returns the schema of the converted item.
   *
   * @return The schema of the converted item.
   */
  public abstract T getDataType();

  /**
   * The extension URL if the field is an extension, null otherwise.
   *
   * @return extension URL if the field is an extension, null otherwise.
   */
  public String extensionUrl() {
    return null;
  }

  /**
   * The FHIR type of the element to be converted, or null if there is no FHIR type, such as within
   * a FHIR backbone element.
   *
   * @return FHIR type of the element to be converted.
   */
  public String getElementType() {
    return null;
  }

  /**
   * Returns a field setter to be used when converting an object of an alternative model to HAPI.
   * Choice types may have multiple element definitions, but in the common case there will be only
   * one.
   *
   * @param elementDefinitions the set of element definitions that the element can be.
   * @return the field setter.
   */
  public abstract HapiFieldSetter toHapiConverter(
      BaseRuntimeElementDefinition... elementDefinitions);
}
