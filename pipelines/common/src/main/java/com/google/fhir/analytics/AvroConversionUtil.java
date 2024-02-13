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
import com.cerner.bunsen.ProfileMapperFhirContexts;
import com.cerner.bunsen.avro.AvroConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is supposed to hold all AvroConverter objects, i.e., for all resource types. It is
 * also intended to be a singleton because creation of these objects can be expensive. So any user
 * that needs an AvroConverter gets the single instance of this class (per JVM) using `getInstance`.
 */
public class AvroConversionUtil {

  private static final Logger log = LoggerFactory.getLogger(AvroConversionUtil.class);

  // This is the singleton instance.
  private static AvroConversionUtil instance;

  private final Map<FhirVersionEnum, Map<String, AvroConverter>> converterMap;

  private ProfileMapperFhirContexts profileMapperFhirContexts;
  /**
   * This is to fix the logical type conversions for BigDecimal. This should be called once before
   * any FHIR resource conversion to Avro.
   */
  public static void initializeAvroConverters() {
    // For more context on the next two conversions, see this thread: https://bit.ly/3iE4rwS
    // Add BigDecimal conversion to the singleton instance to fix "Unknown datum type" Avro
    // exception.
    GenericData.get().addLogicalTypeConversion(new DecimalConversion());
    // This is for a similar error in the ParquetWriter.write which uses SpecificData.get() as its
    // model.
    SpecificData.get().addLogicalTypeConversion(new DecimalConversion());
  }

  private AvroConversionUtil() {
    this.converterMap = Maps.newHashMap();
    this.profileMapperFhirContexts = ProfileMapperFhirContexts.getInstance();
  }

  static synchronized AvroConversionUtil getInstance() {
    if (instance == null) {
      instance = new AvroConversionUtil();
    }
    return instance;
  }

  synchronized AvroConversionUtil loadContextFor(
      FhirVersionEnum fhirVersionEnum, @Nullable String profileDefinitionsDir) {
    profileMapperFhirContexts.contextFor(fhirVersionEnum, profileDefinitionsDir);
    return this;
  }

  synchronized AvroConverter getConverter(String resourceType, FhirContext fhirContext) {
    FhirVersionEnum fhirVersionEnum = fhirContext.getVersion().getVersion();
    Map<String, AvroConverter> map =
        converterMap.computeIfAbsent(fhirVersionEnum, key -> new HashMap<>());
    if (!map.containsKey(resourceType)) {
      String profile =
          profileMapperFhirContexts.getMappedProfileForResource(fhirVersionEnum, resourceType);
      if (Strings.isNullOrEmpty(profile)) {
        String errorMsg =
            String.format("No mapped profile found for resourceType=%s", resourceType);
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg);
      }
      AvroConverter converter = AvroConverter.forResource(fhirContext, profile);
      map.put(resourceType, converter);
    }
    return map.get(resourceType);
  }

  @VisibleForTesting
  @Nullable
  GenericRecord convertToAvro(Resource resource, FhirContext fhirContext) {
    AvroConverter converter = getConverter(resource.getResourceType().name(), fhirContext);
    // TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
    return (GenericRecord) converter.resourceToAvro(resource);
  }

  @VisibleForTesting
  Resource convertToHapi(GenericRecord record, String resourceType, FhirContext fhirContext) {
    // Note resourceType can also be inferred from the record (through fhirType).
    AvroConverter converter = getConverter(resourceType, fhirContext);
    IBaseResource resource = converter.avroToResource(record);
    // TODO: fix this for other FHIR versions: https://github.com/google/fhir-data-pipes/issues/400
    if (!(resource instanceof Resource)) {
      throw new IllegalArgumentException("Cannot convert input record to resource!");
    }
    return (Resource) resource;
  }

  public Schema getResourceSchema(String resourceType, FhirContext fhirContext) {
    AvroConverter converter = getConverter(resourceType, fhirContext);
    Schema schema = converter.getSchema();
    log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
    return schema;
  }

  public List<GenericRecord> generateRecords(Bundle bundle, FhirContext fhirContext) {
    List<GenericRecord> records = new ArrayList<>();
    if (bundle.getTotal() == 0) {
      return records;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      GenericRecord record = convertToAvro(resource, fhirContext);
      if (record != null) {
        records.add(record);
      }
    }
    return records;
  }

  @VisibleForTesting
  synchronized AvroConversionUtil deRegisterMappingsFor(FhirVersionEnum fhirVersionEnum) {
    if (profileMapperFhirContexts != null) {
      profileMapperFhirContexts.deRegisterFhirContexts(fhirVersionEnum);
    }
    if (converterMap != null) {
      converterMap.remove(fhirVersionEnum);
    }
    return this;
  }
}
