package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import com.cerner.bunsen.avro.converters.DefinitionToAvroVisitor;
import com.cerner.bunsen.definitions.HapiConverter;
import com.cerner.bunsen.definitions.HapiConverter.HapiObjectConverter;
import com.cerner.bunsen.definitions.StructureDefinitions;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.jspecify.annotations.Nullable;

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
      Map<String, HapiConverter<Schema>> compositeConverters,
      int recursiveDepth) {

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
            structureDefinitions.conversionSupport(),
            basePackage,
            compositeConverters,
            recursiveDepth);

    HapiConverter<Schema> converter =
        structureDefinitions.transform(visitor, resourceTypeUrl, containedResourceTypeUrls);

    RuntimeResourceDefinition[] resources =
        new RuntimeResourceDefinition[1 + containedResourceTypeUrls.size()];

    Preconditions.checkNotNull(converter.getElementType(), "Converter must have an element type");
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
      FhirContext context, Map<String, List<String>> resourceTypeUrls, int recursiveDepth) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    Map<String, HapiConverter<Schema>> converters = new HashMap<>();

    for (Entry<String, List<String>> resourceTypeUrlEntry : resourceTypeUrls.entrySet()) {

      visitResource(
          context,
          structureDefinitions,
          resourceTypeUrlEntry.getKey(),
          resourceTypeUrlEntry.getValue(),
          converters,
          recursiveDepth);
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
   * @param recursiveDepth the maximum recursive depth to stop when converting a FHIR
   *     StructureDefinition to an Avro schema.
   * @return an Avro converter instance.
   */
  public static AvroConverter forResource(
      FhirContext context, String resourceTypeUrl, int recursiveDepth) {

    return forResource(context, resourceTypeUrl, Collections.emptyList(), recursiveDepth);
  }

  /**
   * Similar to {@link #forResource(FhirContext, String, int)} this method returns an Avro
   * converter, but the returned Avro converter is a union of all the converters for the given
   * resourceTypeUrls, the union is formed by merging all the fields in each converter.
   *
   * @param context the FHIR context
   * @param resourceTypeUrls the list of resource type profile urls. The resourceTypeUrl can either
   *     be a relative URL for a base resource (e.g., "Condition" or "Observation"), or a URL
   *     identifying the structure definition for a given profile, such as
   *     "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".
   * @param recursiveDepth the maximum recursive depth to stop when converting a FHIR
   *     StructureDefinition to an Avro schema.
   * @return the merged Avro converter
   */
  public static AvroConverter forResources(
      FhirContext context, List<String> resourceTypeUrls, int recursiveDepth)
      throws ProfileException {
    List<AvroConverter> avroConverters = new ArrayList<>();
    for (String resourceTypeUrl : resourceTypeUrls) {
      AvroConverter avroConverter =
          forResource(context, resourceTypeUrl, Collections.emptyList(), recursiveDepth);
      avroConverters.add(avroConverter);
    }
    return mergeAvroConverters(avroConverters, context);
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
   * @param recursiveDepth the maximum recursive depth to stop when converting a FHIR
   *     StructureDefinition to an Avro schema.
   * @return an Avro converter instance.
   */
  public static AvroConverter forResource(
      FhirContext context,
      String resourceTypeUrl,
      List<String> containedResourceTypeUrls,
      int recursiveDepth) {

    StructureDefinitions structureDefinitions = StructureDefinitions.create(context);

    return visitResource(
        context,
        structureDefinitions,
        resourceTypeUrl,
        containedResourceTypeUrls,
        new HashMap<>(),
        recursiveDepth);
  }

  /**
   * Converts a given FHIR resource to an Avro {@link IndexedRecord}.
   *
   * @param resource the FHIR resource
   * @return the record.
   */
  @Nullable
  public IndexedRecord resourceToAvro(IBaseResource resource) {

    return (IndexedRecord) hapiToAvroConverter.fromHapi(resource);
  }

  /**
   * Converts a given Avro {@link IndexedRecord} to a FHIR resource.
   *
   * @param record the record
   * @return the FHIR resource.
   */
  @Nullable
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
  @Nullable
  public String getResourceType() {
    return hapiToAvroConverter.getElementType();
  }

  /**
   * Merges all the given list of avroConverters to create a single AvroConverter which is a union
   * of all the fields in the list of avroConverters
   */
  private static AvroConverter mergeAvroConverters(
      List<AvroConverter> avroConverters, FhirContext context) throws ProfileException {
    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(avroConverters), "AvroConverter list cannot be empty for merging");
    Iterator<AvroConverter> iterator = avroConverters.iterator();
    AvroConverter mergedConverter = iterator.next();
    while (iterator.hasNext()) {
      mergedConverter = mergeAvroConverters(mergedConverter, iterator.next(), context);
    }
    return mergedConverter;
  }

  private static AvroConverter mergeAvroConverters(
      AvroConverter left, AvroConverter right, FhirContext context) throws ProfileException {
    HapiConverter<Schema> mergedConverter =
        left.hapiToAvroConverter.merge(right.hapiToAvroConverter);
    RuntimeResourceDefinition[] resources = new RuntimeResourceDefinition[1];
    resources[0] = context.getResourceDefinition(mergedConverter.getElementType());
    return new AvroConverter(mergedConverter, resources);
  }
}
