package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Helper functions to allow code to convert FHIR resources independently of the FHIR version.
 * Typically an implementation specific to a FHIR version is provided at runtime.
 */
public abstract class FhirConversionSupport implements Serializable {

  private static final String STU3_SUPPORT_CLASS =
      "com.cerner.bunsen.definitions.stu3.Stu3FhirConversionSupport";

  /**
   * Returns the type of a given FHIR object, such as "Condition" or "Observation".
   *
   * @param base a FHIR object
   * @return the FHIR type
   */
  public abstract String fhirType(IBase base);

  /**
   * Returns a map of the fields in a composite type to the corresponding values. Since FHIR fields
   * may have multiple values a list is returned, and single-valued items will have a singleton
   * list.
   *
   * @param composite a FHIR composite type
   * @return a map of field names to the values.
   */
  public abstract Map<String, List> compositeValues(IBase composite);

  /**
   * Extracts resources of the given type from a FHIR bundle.
   *
   * @param bundle the bundle
   * @param resourceType the resource type name, such as "Condition" or "Observation"
   * @return the resources of the specified type.
   */
  public abstract java.util.List<IBaseResource> extractEntryFromBundle(
      IBaseBundle bundle, String resourceType);

  /** Cache of FHIR contexts. */
  private static final Map<FhirVersionEnum, FhirConversionSupport> FHIR_SUPPORT = new HashMap();

  private static FhirConversionSupport newInstance(FhirVersionEnum fhirVersion) {

    Class fhirSupportClass;

    if (FhirVersionEnum.DSTU3.equals(fhirVersion)) {

      try {
        fhirSupportClass = Class.forName(STU3_SUPPORT_CLASS);

      } catch (ClassNotFoundException exception) {

        throw new IllegalStateException(exception);
      }

    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: " + fhirVersion);
    }

    try {

      return (FhirConversionSupport) fhirSupportClass.newInstance();

    } catch (Exception exception) {

      throw new IllegalStateException("Unable to create FHIR support class", exception);
    }
  }

  /**
   * Returns the FHIR context for the given version. This is effectively a cache so consuming code
   * does not need to recreate the context repeatedly.
   *
   * @param fhirVersion the version of FHIR to use
   * @return the FhirContext
   */
  public static FhirConversionSupport supportFor(FhirVersionEnum fhirVersion) {

    synchronized (FHIR_SUPPORT) {
      FhirConversionSupport support = FHIR_SUPPORT.get(fhirVersion);

      if (support == null) {

        support = newInstance(fhirVersion);

        FHIR_SUPPORT.put(fhirVersion, support);
      }

      return support;
    }
  }

  /**
   * Convenience function to load support for FHIR STU3.
   *
   * @return the conversion support instance.
   */
  public static FhirConversionSupport forStu3() {

    return supportFor(FhirVersionEnum.DSTU3);
  }
}
