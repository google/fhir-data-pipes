package com.cerner.bunsen.definitions;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.support.IValidationSupport;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Set;

/**
 * Abstract base class to visit FHIR structure definitions.
 */
public abstract class StructureDefinitions {

  protected static final Set<String> PRIMITIVE_TYPES =  ImmutableSet.<String>builder()
      .add("id")
      .add("boolean")
      .add("code")
      .add("markdown")
      .add("date")
      .add("instant")
      .add("datetime")
      .add("dateTime")
      .add("time")
      .add("oid")
      .add("string")
      .add("decimal")
      .add("integer")
      .add("xhtml")
      .add("unsignedInt")
      .add("positiveInt")
      .add("base64Binary")
      .add("uri")
      // TODO: Figure out why these are added to R4 resource definitions.
      .add("http://hl7.org/fhirpath/System.Boolean")
      .add("http://hl7.org/fhirpath/System.String")
      .add("http://hl7.org/fhirpath/System.Integer")
      .add("http://hl7.org/fhirpath/System.Long")
      .add("http://hl7.org/fhirpath/System.Decimal")
      .add("http://hl7.org/fhirpath/System.DateTime")
      .add("http://hl7.org/fhirpath/System.Time")
      .add("http://hl7.org/fhirpath/System.Quantity")
      .add("canonical")
      .add("url")
      .build();

  private static final String STU3_DEFINITIONS_CLASS =
      "com.cerner.bunsen.definitions.stu3.Stu3StructureDefinitions";

  private static final String R4_DEFINITIONS_CLASS =
      "com.cerner.bunsen.definitions.r4.R4StructureDefinitions";

  protected final FhirContext context;

  protected final IValidationSupport validationSupport;

  /**
   * Creates a new instance with the given context.
   *
   * @param context the FHIR context.
   */
  public StructureDefinitions(FhirContext context) {

    this.context = context;
    this.validationSupport = context.getValidationSupport();
  }

  /**
   * Transforms a FHIR resource to a type defined by the visitor.
   *
   * @param visitor a visitor class to recursively transform the structure.
   * @param resourceTypeUrl the URL defining the resource type or profile.
   * @param <T> the return type of the visitor.
   * @return the transformed result.
   */
  public abstract <T> T transform(DefinitionVisitor<T> visitor,
      String resourceTypeUrl);

  /**
   * Transforms a FHIR resource to a type defined by the visitor.
   * @param visitor a visitor class to recursively transform the structure.
   * @param resourceTypeUrl the URL defining the resource type or profile.
   * @param containedResourceTypeUrls the URLs defining the resource types or profiles to be
   *        contained to the given resource.
   * @param <T> the return type of the visitor.
   * @return the transformed result.
   */
  public abstract <T> T transform(DefinitionVisitor<T> visitor,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls);

  /**
   * Returns supporting functions to make FHIR conversion work independent of version.
   *
   * @return functions supporting FHIR conversion.
   */
  public abstract FhirConversionSupport conversionSupport();


  /**
   * Create a new instance of this class for the given version of FHIR.
   *
   * @param context The FHIR context
   * @return a StructureDefinitions instance.
   */
  public static StructureDefinitions create(FhirContext context) {

    Class structureDefinitionsClass;

    FhirVersionEnum versionEnum = context.getVersion().getVersion();
    String className = null;

    if (FhirVersionEnum.DSTU3.equals(versionEnum)) {
      className = STU3_DEFINITIONS_CLASS;
    } else if (FhirVersionEnum.R4.equals(versionEnum)) {
      className = R4_DEFINITIONS_CLASS;
    } else {
      throw new IllegalArgumentException("Unsupported FHIR version: " + versionEnum);
    }

    try {
      structureDefinitionsClass = Class.forName(className);
    } catch (ClassNotFoundException exception) {
      throw new IllegalStateException(exception);
    }

    try {
      Constructor constructor = structureDefinitionsClass.getConstructor(FhirContext.class);
      return (StructureDefinitions) constructor.newInstance(context);
    } catch (Exception exception) {
      throw new IllegalStateException(exception);
    }
  }

}
