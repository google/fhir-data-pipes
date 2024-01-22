/*
 * Copyright 2020-2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.fhir.analytics;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.generic.GenericRecord;
import org.hl7.fhir.r4.model.Resource;

/** Utility class containing helper methods for converting FHIR resources to Avro records. */
public class AvroUtil {
  private static Map<FhirVersionEnum, Map<String, AvroConverter>> avroConverterMap =
      new ConcurrentHashMap<>();

  /**
   * Converts the given FHIR {@code resource} to an Avro record. The Avro converter that will be
   * applied is returned by the method {@link #getConverter(String, FhirContext)}
   */
  public static GenericRecord convertToAvro(Resource resource, FhirContext fhirContext) {
    String resourceType = resource.getResourceType().name();
    AvroConverter avroConverter = getConverter(resourceType, fhirContext);
    // TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
    return (GenericRecord) avroConverter.resourceToAvro(resource);
  }

  /**
   * Retrieves the {@link AvroConverter} that needs to be used for conversion for the given {@code
   * resourceType}. The {@link AvroConverter} will be retrieved based on the profile that is mapped
   * for the resourceType.
   */
  public static synchronized AvroConverter getConverter(
      String resourceType, FhirContext fhirContext) {
    FhirVersionEnum fhirVersionEnum = fhirContext.getVersion().getVersion();
    Map<String, AvroConverter> map =
        avroConverterMap.computeIfAbsent(fhirVersionEnum, key -> new HashMap<>());
    if (!map.containsKey(resourceType)) {
      Map<String, String> mapping =
          FhirContexts.resourceProfileMappingsFor(fhirContext.getVersion().getVersion());
      String profile = mapping.get(resourceType);
      AvroConverter converter = AvroConverter.forResource(fhirContext, profile);
      map.put(resourceType, converter);
    }
    return map.get(resourceType);
  }

  /** Removes the mappings cached against the given fhirVersionEnum. */
  public static synchronized void deRegisterConverters(FhirVersionEnum fhirVersionEnum) {
    avroConverterMap.remove(fhirVersionEnum);
  }
}
