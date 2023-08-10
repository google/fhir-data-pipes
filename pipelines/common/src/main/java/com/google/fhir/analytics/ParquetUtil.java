/*
 * Copyright 2020-2023 Google LLC
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import javax.annotation.Nullable;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetUtil {

  private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);

  public static String PARQUET_EXTENSION = ".parquet";

  private final FhirContext fhirContext;

  private static final Map<String, AvroConverter> converterMap;

  static {
    converterMap = Maps.newHashMap();
  }

  private final Map<String, ParquetWriter<GenericRecord>> writerMap;

  private final int rowGroupSize;

  private final DwhFiles dwhFiles;

  private final Timer timer;

  private final Random random;

  private final String namePrefix;

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

  public String getParquetPath() {
    return dwhFiles.getRoot();
  }

  /**
   * Note using this constructor may cause the files not be flushed until `closeAllWriters` is
   * called.
   *
   * @param fhirVersionEnum the FHIR version
   * @param parquetFilePath The directory under which the Parquet files are written.
   */
  public ParquetUtil(FhirVersionEnum fhirVersionEnum, String parquetFilePath) {
    this(fhirVersionEnum, parquetFilePath, 0, 0, "", FileSystems.getDefault());
  }

  /**
   * The preferred constructor for the streaming mode.
   *
   * @param fhirVersionEnum This should match the resources intended to be converted.
   * @param parquetFilePath The directory under which the Parquet files are written.
   * @param secondsToFlush The interval after which the content of Parquet writers is flushed to
   *     disk.
   * @param rowGroupSize The approximate size of row-groups in the Parquet files (0 means use
   *     default).
   */
  public ParquetUtil(
      FhirVersionEnum fhirVersionEnum,
      String parquetFilePath,
      int secondsToFlush,
      int rowGroupSize,
      String namePrefix) {
    this(
        fhirVersionEnum,
        parquetFilePath,
        secondsToFlush,
        rowGroupSize,
        namePrefix,
        FileSystems.getDefault());
  }

  // TODO remove this constructor and only expose a similar one in `DwhFiles` (for testing).
  @VisibleForTesting
  ParquetUtil(
      FhirVersionEnum fhirVersionEnum,
      String parquetFilePath,
      int secondsToFlush,
      int rowGroupSize,
      String namePrefix,
      FileSystem fileSystem) {
    if (fhirVersionEnum == FhirVersionEnum.DSTU3 || fhirVersionEnum == FhirVersionEnum.R4) {
      this.fhirContext = FhirContexts.contextFor(fhirVersionEnum);
    } else {
      throw new IllegalArgumentException("Only versions 3 and 4 of FHIR are supported!");
    }
    this.dwhFiles = new DwhFiles(parquetFilePath, fhirContext);
    this.writerMap = new HashMap<>();
    this.rowGroupSize = rowGroupSize;
    this.namePrefix = namePrefix;
    if (secondsToFlush > 0) {
      TimerTask task =
          new TimerTask() {

            @Override
            public void run() {
              try {
                log.info(
                    "Flushing all Parquet writers for thread " + Thread.currentThread().getId());
                flushAll();
              } catch (IOException e) {
                log.error("Could not flush Parquet files: " + e);
              }
            }
          };
      this.timer = new Timer();
      timer.scheduleAtFixedRate(task, secondsToFlush * 1000, secondsToFlush * 1000);
    } else {
      timer = null;
    }
    this.random = new Random(System.currentTimeMillis());
  }

  private static synchronized AvroConverter getConverter(
      String resourceType, FhirContext fhirContext) {
    if (!converterMap.containsKey(resourceType)) {
      // TODO: Check how to automate discovery of relevant profiles to be applied. Right now we need
      // to supply the corresponding resource profile identifier for the extensions to work, e.g.,
      // "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient" instead of "Patient".
      // https://github.com/google/fhir-data-pipes/issues/560
      AvroConverter converter = AvroConverter.forResource(fhirContext, resourceType);
      converterMap.put(resourceType, converter);
    }
    return converterMap.get(resourceType);
  }

  @VisibleForTesting
  synchronized ResourceId getUniqueOutputFilePath(String resourceType) throws IOException {
    ResourceId resourceId = dwhFiles.getResourcePath(resourceType);
    String uniquetFileName =
        String.format(
            "%s%s_output-parquet-th-%d-ts-%d-r-%d%s",
            namePrefix,
            resourceType,
            Thread.currentThread().getId(),
            System.currentTimeMillis(),
            random.nextInt(1000000),
            PARQUET_EXTENSION);
    return resourceId.resolve(uniquetFileName, StandardResolveOptions.RESOLVE_FILE);
  }

  private synchronized void createWriter(String resourceType) throws IOException {
    // TODO: Find a way to convince Hadoop file operations to use `fileSystem` (needed for testing).

    ResourceId resourceId = getUniqueOutputFilePath(resourceType);
    WritableByteChannel writableByteChannel =
        org.apache.beam.sdk.io.FileSystems.create(resourceId, MimeTypes.BINARY);
    OutputStream outputStream = Channels.newOutputStream(writableByteChannel);
    FhirOutputFile fhirOutputFile = new FhirOutputFile(outputStream);

    AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(fhirOutputFile);
    // TODO: Adjust other parquet file parameters for our needs or make them configurable.
    if (rowGroupSize > 0) {
      builder.withRowGroupSize(rowGroupSize);
    }
    ParquetWriter<GenericRecord> writer =
        builder.withSchema(getResourceSchema(resourceType, fhirContext)).build();
    writerMap.put(resourceType, writer);
  }

  /**
   * This is to write a FHIR resource to a Parquet file. This automatically handles file creation in
   * a directory named after the resource type (e.g., `Patient`) with names following the
   * "output-streaming-ddddd" pattern where `ddddd` is a string with five digits. NOTE: This should
   * not be used in its current form once we move the streaming pipeline to Beam; the I/O should be
   * left to Beam similar to the batch mode.
   */
  public synchronized void write(Resource resource) throws IOException {
    Preconditions.checkNotNull(resource.fhirType());
    String resourceType = resource.fhirType();
    if (!writerMap.containsKey(resourceType)) {
      createWriter(resourceType);
    }
    final ParquetWriter<GenericRecord> parquetWriter = writerMap.get(resourceType);
    GenericRecord record = convertToAvro(resource);
    if (record != null) {
      parquetWriter.write(record);
    }
  }

  private synchronized void flush(String resourceType) throws IOException {
    ParquetWriter<GenericRecord> writer = writerMap.get(resourceType);
    if (writer != null && writer.getDataSize() > 0) {
      writer.close();
      createWriter(resourceType);
    }
  }

  private synchronized void flushAll() throws IOException {
    for (String resourceType : writerMap.keySet()) {
      flush(resourceType);
    }
  }

  public synchronized void closeAllWriters() throws IOException {
    if (timer != null) {
      timer.cancel();
    }
    for (Map.Entry<String, ParquetWriter<GenericRecord>> entry : writerMap.entrySet()) {
      entry.getValue().close();
      writerMap.put(entry.getKey(), null);
    }
  }

  public static Schema getResourceSchema(String resourceType, FhirVersionEnum fhirVersion) {
    return getResourceSchema(resourceType, FhirContexts.contextFor(fhirVersion));
  }

  public static Schema getResourceSchema(String resourceType, FhirContext fhirContext) {
    AvroConverter converter = getConverter(resourceType, fhirContext);
    Schema schema = converter.getSchema();
    log.debug(String.format("Schema for resource type %s is %s", resourceType, schema));
    return schema;
  }

  public List<GenericRecord> generateRecords(Bundle bundle) {
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

  public void writeRecords(Bundle bundle, Set<String> resourceTypes) throws IOException {
    if (bundle.getEntry() == null) {
      return;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      if (resourceTypes == null || resourceTypes.contains(resource.getResourceType().name())) {
        write(resource);
      }
    }
  }

  @VisibleForTesting
  @Nullable
  GenericRecord convertToAvro(Resource resource) {
    AvroConverter converter = getConverter(resource.getResourceType().name(), fhirContext);
    // TODO: Check why Bunsen returns IndexedRecord instead of GenericRecord.
    return (GenericRecord) converter.resourceToAvro(resource);
  }
}
