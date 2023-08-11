package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.avro.converters.DefinitionToAvroVisitor;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.StructureDefinitions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.hl7.fhir.instance.model.api.IBaseResource;

/** Converter to change HAPI objects into Avro structures and vice versa. */
public class AvroConverter {

  private final HapiConverter<Schema> hapiToAvroConverter;

  private final HapiObjectConverter avroToHapiConverter;

  private AvroConverter(
      HapiConverter<Schema> hapiToAvroConverter, RuntimeResourceDefinition... resources) {

    this.hapiToAvroConverter = hapiToAvroConverter;
    this.avroToHapiConverter = (HapiObjectConverter) hapiToAvroConverter.toHapiConverter(resources);
  }

  private static AvroConverter visitResource(
      FhirContext context,
      StructureDefinitions structureDefinitions,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls,
      Map<String, HapiConverter<Schema>> compositeConverters) {

    FhirVersionEnum fhirVersion = context.getVersion().getVersion();

    String basePackage;

    if (FhirVersionEnum.DSTU3.equals(fhirVersion)) {

      basePackage = "com.cerner.bunsen.stu3.avro";

    } else if (FhirVersionEnum.R4.equals(fhirVersion)) {

      basePackage = "com.cerner.bunsen.r4.avro";

    } else {

      throw new IllegalArgumentException("Unsupported FHIR version " + fhirVersion.toString());
    }

    DefinitionToAvroVisitor visitor =
        new DefinitionToAvroVisitor(
            structureDefinitions.conversionSupport(), basePackage, compositeConverters);

    HapiConverter<Schema> converter =
        structureDefinitions.transform(visitor, resourceTypeUrl, containedResourceTypeUrls);

    RuntimeResourceDefinition[] resources =
        new RuntimeResourceDefinition[1 + containedResourceTypeUrls.size()];

    resources[0] = context.getResourceDefinition(converter.getElementType());

    for (int i = 0; i < containedResourceTypeUrls.size(); i++) {

      // Retrieve the name of the contained resources from the Resource Container's schema
      Field containedField =
          converter
              .getDataType()
              .getField("contained")
              .schema()
              .getTypes()
              .get(1) // Get non-null element of the Union
              .getElementType()
              .getFields()
              .get(i);

      // Shift array by 1, since the 0 index holds the parent definition
      resources[i + 1] = context.getResourceDefinition(containedField.name());
    }

    return new AvroConverter(converter, resources);
  }

  /**
   * Returns a list of Avro schemas to support the given FHIR resource types.
   *
   * @param context the FHIR context
   * @param resourceTypeUrls the URLs of the resource types and any resources URLs contained to them
   * @return a list of Avro schemas
   */
  public static List<Schema> generateSchemas(
      FhirContext context, Map<String, List<String>> resourceTypeUrls) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    Map<String, HapiConverter<Schema>> converters = new HashMap<>();

    for (Entry<String, List<String>> resourceTypeUrlEntry : resourceTypeUrls.entrySet()) {

      visitResource(
          context,
          structureDefinitions,
          resourceTypeUrlEntry.getKey(),
          resourceTypeUrlEntry.getValue(),
          converters);
    }

    return converters.values().stream()
        .map(HapiConverter::getDataType)
        .collect(Collectors.toList());
  }

  /**
   * Returns an Avro converter for the given resource type. The resource type can either be a
   * relative URL for a base resource (e.g., "Condition" or "Observation"), or a URL identifying the
   * structure definition for a given profile, such as
   * "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   *
   * @param context the FHIR context
   * @param resourceTypeUrl the URL of the resource type
   * @return an Avro converter instance.
   */
  public static AvroConverter forResource(FhirContext context, String resourceTypeUrl) {

    return forResource(context, resourceTypeUrl, Collections.emptyList());
  }

  /**
   * Returns an Avro converter for the given resource type. The resource type can either be a
   * relative URL for a base resource (e.g. "Condition" or "Observation"), or a URL identifying the
   * structure definition for a given profile, such as
   * "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   *
   * <p>Resources that would be contained must be statically declared through this method via
   * similar URLs.
   *
   * @param context the FHIR context
   * @param resourceTypeUrl the URL of the resource type
   * @param containedResourceTypeUrls the list of URLs of contained resource types
   * @return an Avro converter instance.
   */
  public static AvroConverter forResource(
      FhirContext context, String resourceTypeUrl, List<String> containedResourceTypeUrls) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    return visitResource(
        context, structureDefinitions, resourceTypeUrl, containedResourceTypeUrls, new HashMap<>());
  }

  /**
   * Converts a given FHIR resource to an Avro {@link IndexedRecord}.
   *
   * @param resource the FHIR resource
   * @return the record.
   */
  public IndexedRecord resourceToAvro(IBaseResource resource) {

    return (IndexedRecord) hapiToAvroConverter.fromHapi(resource);
  }

  /**
   * Converts a given Avro {@link IndexedRecord} to a FHIR resource.
   *
   * @param record the record
   * @return the FHIR resource.
   */
  public IBaseResource avroToResource(IndexedRecord record) {

    return (IBaseResource) avroToHapiConverter.toHapi(record);
  }

  /**
   * Returns the Avro schema equivalent for the FHIR resource.
   *
   * @return the Avro schema
   */
  public Schema getSchema() {

    return hapiToAvroConverter.getDataType();
  }

  /**
   * Returns the FHIR type of the resource being converted.
   *
   * @return the FHIR type of the resource being converted.
   */
  public String getResourceType() {
    return hapiToAvroConverter.getElementType();
  }
}
