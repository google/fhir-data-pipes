package com.cerner.bunsen.avro;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.instance.model.api.IBaseResource;

public class TestUtil {

  /**
   * This is used when we want to verify that no information is lost in the process of converting to
   * Avro records. The idea is to convert the resource to the JSON representation then parse it back
   * to a HAPI object and verify we get the original HAPI object. This is needed because HAPI adds
   * some information, like `resourceType` in `id` fields, when parsing from JSON but, we don't have
   * that in the `id` field of converted Avro fields. See:
   * https://github.com/google/fhir-data-pipes/issues/1003
   *
   * @param resource the resource which is created from an Avro record.
   * @param resourceClass the Class of the resource.
   * @param fhirContext the FHIR context.
   * @return a HAPI object created from parsing the encoded version of `resource`.
   */
  public static IBaseResource encodeThenParse(
      IBaseResource resource,
      Class<? extends IBaseResource> resourceClass,
      FhirContext fhirContext) {
    IParser jsonParser = fhirContext.newJsonParser();
    return jsonParser.parseResource(resourceClass, jsonParser.encodeResourceToString(resource));
  }
}
