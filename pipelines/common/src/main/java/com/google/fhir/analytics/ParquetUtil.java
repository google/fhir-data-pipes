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

import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.exception.ProfileException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Cleaner;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.avro.generic.GenericRecord;
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

// Implementing AutoCloseable does not help much in the usage pattern we have with DoFns.
public class ParquetUtil {

  private static final Logger log = LoggerFactory.getLogger(ParquetUtil.class);
  public static String PARQUET_EXTENSION = ".parquet";
  private final AvroConversionUtil conversionUtil;
  private final Map<String, ParquetWriter<GenericRecord>> writerMap;
  private final int rowGroupSize;
  private final DwhFiles dwhFiles;
  private final Timer timer;
  private final Random random;
  private final String namePrefix;
  private boolean flushedInCurrentPeriod;
  private static final Cleaner cleaner;

  static {
    cleaner = Cleaner.create();
  }

  // TODO this is a terrible method to force closure of Parquet files; this is
  //  done for demonstration only and should be removed before merge! The same
  //  can be achieved by using `finalize()` which is deprecated since Java 9.
  private static class CleaningAction implements Runnable {
    private Map<String, ParquetWriter<GenericRecord>> writerMap;
    private Timer timer;

    CleaningAction(Map<String, ParquetWriter<GenericRecord>> writerMap, Timer timer) {
      // We cannot keep references to the ParquetUtil itself see:
      // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/ref/Cleaner.html
      this.writerMap = writerMap;
      this.timer = timer;
    }

    @Override
    public void run() {
      try {
        log.info("******* DEBUG in run() BEFORE ********");
        ParquetUtil.closeAllWriters(writerMap, timer);
        log.info("******* DEBUG in run() AFTER ********");
      } catch (IOException e) {
        // There is not much else we can do at this stage.
        log.error("IOException during cleaning: ", e);
      }
    }
  }

  private synchronized void setFlushedInCurrentPeriod(boolean value) {
    flushedInCurrentPeriod = value;
  }

  public String getParquetPath() {
    return dwhFiles.getRoot();
  }

  // TODO remove this constructor and only expose a similar one in `DwhFiles` (for testing).
  /**
   * @param fhirVersionEnum This should match the resources intended to be converted.
   * @param structureDefinitionsPath Directory path containing the structure definitions for custom
   *     fhir profiles; if it starts with `classpath:` then classpath will be searched instead.
   * @param parquetFilePath The directory under which the Parquet files are written.
   * @param secondsToFlush The interval after which the content of Parquet writers is flushed to
   *     disk.
   * @param rowGroupSize The approximate size of row-groups in the Parquet files (0 means use
   *     default).
   * @param namePrefix The prefix directory at which the Parquet files are written
   * @throws ProfileException if any errors are encountered during initialisation of FhirContext
   */
  @VisibleForTesting
  ParquetUtil(
      FhirVersionEnum fhirVersionEnum,
      String structureDefinitionsPath,
      String parquetFilePath,
      int secondsToFlush,
      int rowGroupSize,
      String namePrefix,
      int recursiveDepth)
      throws ProfileException {
    if (fhirVersionEnum == FhirVersionEnum.DSTU3 || fhirVersionEnum == FhirVersionEnum.R4) {
      this.conversionUtil =
          AvroConversionUtil.getInstance(fhirVersionEnum, structureDefinitionsPath, recursiveDepth);
    } else {
      throw new IllegalArgumentException("Only versions 3 and 4 of FHIR are supported!");
    }
    this.dwhFiles = new DwhFiles(parquetFilePath, conversionUtil.getFhirContext());
    this.writerMap = new HashMap<>();
    this.rowGroupSize = rowGroupSize;
    this.namePrefix = namePrefix;
    setFlushedInCurrentPeriod(false);
    // TODO remove timer and secondsToFlush option if using the `cleaner` is desired; note this
    //  holds pointers to the enclosing ParqeutUtil instance hence the `clean` is never called!
    if (secondsToFlush > 0) {
      TimerTask task =
          new TimerTask() {

            @Override
            public void run() {
              try {
                // If a flush has happened recently, e.g., through a `FinishBundle` call, we don't
                // need to do that again; the timer here is just to make sure the data is flushed
                // into Parquet files _at least_ once in every `secondsToFlush`.
                if (!flushedInCurrentPeriod) {
                  log.info("Flush timed out for thread " + Thread.currentThread().getId());
                  flushAll();
                }
                setFlushedInCurrentPeriod(false);
              } catch (IOException | ProfileException e) {
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
    cleaner.register(this, new CleaningAction(writerMap, timer));
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

  private synchronized void createWriter(String resourceType) throws IOException, ProfileException {

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
        builder.withSchema(conversionUtil.getResourceSchema(resourceType)).build();
    writerMap.put(resourceType, writer);
  }

  /**
   * This is to write a FHIR resource to a Parquet file. This automatically handles file creation in
   * a directory named after the resource type (e.g., `Patient`) with names following the
   * "output-streaming-ddddd" pattern where `ddddd` is a string with five digits. NOTE: This should
   * not be used in its current form once we move the streaming pipeline to Beam; the I/O should be
   * left to Beam similar to the batch mode.
   */
  public synchronized void write(Resource resource) throws IOException, ProfileException {
    Preconditions.checkNotNull(resource.fhirType());
    String resourceType = resource.fhirType();
    if (!writerMap.containsKey(resourceType)) {
      createWriter(resourceType);
    }
    final ParquetWriter<GenericRecord> parquetWriter = writerMap.get(resourceType);
    GenericRecord record = conversionUtil.convertToAvro(resource);
    if (record != null) {
      parquetWriter.write(record);
    }
  }

  private synchronized void flush(String resourceType) throws IOException, ProfileException {
    ParquetWriter<GenericRecord> writer = writerMap.get(resourceType);
    if (writer != null && writer.getDataSize() > 0) {
      writer.close();
      createWriter(resourceType);
    }
  }

  synchronized void flushAll() throws IOException, ProfileException {
    log.info("Flushing all Parquet writers for thread " + Thread.currentThread().getId());
    for (String resourceType : writerMap.keySet()) {
      flush(resourceType);
    }
    setFlushedInCurrentPeriod(true);
  }

  private static void closeAllWriters(
      Map<String, ParquetWriter<GenericRecord>> writerMap, Timer timer) throws IOException {
    if (timer != null) {
      synchronized (timer) {
        timer.cancel();
      }
    }
    if (writerMap != null) {
      synchronized (writerMap) {
        for (Map.Entry<String, ParquetWriter<GenericRecord>> entry : writerMap.entrySet()) {
          entry.getValue().close();
          writerMap.put(entry.getKey(), null);
        }
      }
    }
  }

  public void closeAllWriters() throws IOException {
    closeAllWriters(writerMap, timer);
  }

  public void writeRecords(Bundle bundle, Set<String> resourceTypes)
      throws IOException, ProfileException {
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
}
