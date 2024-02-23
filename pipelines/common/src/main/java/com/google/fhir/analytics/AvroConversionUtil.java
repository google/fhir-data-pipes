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
import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.ArrayList;
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

  private final Map<String, AvroConverter> converterMap;

  private ProfileMapperFhirContexts profileMapperFhirContexts;

  private FhirContext fhirContext;

  private FhirVersionEnum fhirVersionEnum;

  private String structureDefinitionsDir;

  private String structureDefinitionsClasspath;

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

  private AvroConversionUtil(
      FhirVersionEnum fhirVersionEnum,
      @Nullable String structureDefinitionsDir,
      @Nullable String structureDefinitionsClasspath)
      throws ProfileMapperException {
    this.fhirVersionEnum = fhirVersionEnum;
    this.structureDefinitionsDir = structureDefinitionsDir;
    this.structureDefinitionsClasspath = structureDefinitionsClasspath;
    this.converterMap = Maps.newHashMap();
    this.profileMapperFhirContexts = ProfileMapperFhirContexts.getInstance();
    if (!Strings.isNullOrEmpty(structureDefinitionsClasspath)) {
      this.fhirContext =
          profileMapperFhirContexts.contextForFromClasspath(
              fhirVersionEnum, structureDefinitionsClasspath);
    } else {
      this.fhirContext =
          profileMapperFhirContexts.contextFor(fhirVersionEnum, structureDefinitionsDir);
    }
  }

  static synchronized AvroConversionUtil getInstance(
      FhirVersionEnum fhirVersionEnum,
      @Nullable String structureDefinitionsDir,
      @Nullable String structureDefinitionsClasspath)
      throws ProfileMapperException {
    Preconditions.checkNotNull(fhirVersionEnum, "fhirVersionEnum cannot be null");
    structureDefinitionsDir = Strings.nullToEmpty(structureDefinitionsDir);
    structureDefinitionsClasspath = Strings.nullToEmpty(structureDefinitionsClasspath);
    if (!Strings.isNullOrEmpty(structureDefinitionsDir)
        && !Strings.isNullOrEmpty(structureDefinitionsClasspath)) {
      String errorMsg =
          String.format(
              "Please configure only one of the parameter between structureDefinitionsDir=%s and"
                  + " structureDefinitionsClasspath=%s, leave both empty if custom profiles are not"
                  + " needed.",
              structureDefinitionsDir, structureDefinitionsClasspath);
      log.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }

    if (instance == null) {
      instance =
          new AvroConversionUtil(
              fhirVersionEnum, structureDefinitionsDir, structureDefinitionsClasspath);
    } else if (!fhirVersionEnum.equals(instance.fhirVersionEnum)
        || !structureDefinitionsDir.equals(instance.structureDefinitionsDir)
        || !structureDefinitionsClasspath.equals(instance.structureDefinitionsClasspath)) {
      String errorMsg =
          String.format(
              "AvroConversionUtil has been initialised with different set of parameters earlier"
                  + " with fhirVersionEnum=%s, structureDefinitionsDir=%s and"
                  + " structureDefinitionsClasspath=%s, compared to what is being passed now with"
                  + " fhirVersionEnum=%s, structureDefinitionsDir=%s and"
                  + " structureDefinitionsClasspath=%s",
              instance.fhirVersionEnum,
              instance.structureDefinitionsDir,
              instance.structureDefinitionsClasspath,
              fhirVersionEnum,
              structureDefinitionsDir,
              structureDefinitionsClasspath);
      log.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }
    return instance;
  }

  public synchronized FhirContext getFhirContext() throws ProfileMapperException {
    // This should never be the case as creation of new instance makes sure the FhirContext is
    // initialised properly.
    if (fhirContext == null) {
      String errorMsg =
          "The fhirContext is not initialised yet. Please initialise the fhirContext using"
              + " the method getInstance(FhirVersionEnum fhirVersion, @Nullable String"
              + " structureDefinitionsDir)";
      log.error(errorMsg);
      throw new ProfileMapperException(errorMsg);
    }
    return fhirContext;
  }

  synchronized AvroConverter getConverter(String resourceType) throws ProfileMapperException {
    if (!converterMap.containsKey(resourceType)) {
      FhirContext fhirContext = getFhirContext();
      String profile =
          profileMapperFhirContexts.getMappedProfileForResource(
              fhirContext.getVersion().getVersion(), resourceType);
      if (Strings.isNullOrEmpty(profile)) {
        String errorMsg =
            String.format("No mapped profile found for resourceType=%s", resourceType);
        log.error(errorMsg);
        throw new ProfileMapperException(errorMsg);
      }
      AvroConverter converter = AvroConverter.forResource(fhirContext, profile);
      converterMap.put(resourceType, converter);
    }
    return converterMap.get(resourceType);
  }

  @VisibleForTesting
  @Nullable
  GenericRecord convertToAvro(Resource resource) throws ProfileMapperException {
    AvroConverter converter = getConverter(resource.getResourceType().name());
    // TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
    return (GenericRecord) converter.resourceToAvro(resource);
  }

  @VisibleForTesting
  Resource convertToHapi(GenericRecord record, String resourceType) throws ProfileMapperException {
    // Note resourceType can also be inferred from the record (through fhirType).
    AvroConverter converter = getConverter(resourceType);
    IBaseResource resource = converter.avroToResource(record);
    // TODO: fix this for other FHIR versions: https://github.com/google/fhir-data-pipes/issues/400
    if (!(resource instanceof Resource)) {
      throw new IllegalArgumentException("Cannot convert input record to resource!");
    }
    return (Resource) resource;
  }

  public Schema getResourceSchema(String resourceType) throws ProfileMapperException {
    AvroConverter converter = getConverter(resourceType);
    Schema schema = converter.getSchema();
    log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
    return schema;
  }

  public List<GenericRecord> generateRecords(Bundle bundle) throws ProfileMapperException {
    List<GenericRecord> records = new ArrayList<>();
    if (bundle.getTotal() == 0) {
      return records;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      GenericRecord record = convertToAvro(resource);
      if (record != null) {
        records.add(record);
      }
    }
    return records;
  }

  @VisibleForTesting
  static synchronized void deRegisterMappingsFor(FhirVersionEnum fhirVersionEnum) {
    instance = null;
    ProfileMapperFhirContexts.getInstance().deRegisterFhirContexts(fhirVersionEnum);
  }
}
