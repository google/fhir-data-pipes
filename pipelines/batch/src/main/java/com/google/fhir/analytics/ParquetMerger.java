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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO add unit-tests for sub-pipelines.

/**
 * Pipelines for merging two FHIR data-warehouses (e.g., an old version and an incremental update)
 * which does deduplication of resources too.
 */
public class ParquetMerger {

  private static final Logger log = LoggerFactory.getLogger(ParquetMerger.class);

  private static String ID_KEY = "id";
  private static String META_KEY = "meta";
  private static String LAST_UPDATED_KEY = "lastUpdated";
  private static String TAG_KEY = "tag";
  private static String SYSTEM_KEY = "system";
  private static String CODE_KEY = "code";

  private static PCollection<KV<String, GenericRecord>> readAndMapToId(
      Pipeline pipeline, DwhFiles dwh, String resourceType) {
    PCollection<GenericRecord> records =
        pipeline.apply(
            ParquetIO.read(ParquetUtil.getResourceSchema(resourceType, FhirVersionEnum.R4))
                .from(
                    String.format(
                        "%s*%s",
                        dwh.getResourcePath(resourceType).toString(),
                        ParquetUtil.PARQUET_EXTENSION)));

    return records.apply(
        ParDo.of(
            new DoFn<GenericRecord, KV<String, GenericRecord>>() {
              @ProcessElement
              public void processElement(
                  @Element GenericRecord record, OutputReceiver<KV<String, GenericRecord>> out) {
                String id = record.get(ID_KEY).toString();
                if (id == null) {
                  throw new IllegalArgumentException(
                      String.format("No %s key found in %s", ID_KEY, record));
                }
                out.output(KV.of(id, record));
              }
            }));
  }

  private static String getUpdateTime(GenericRecord record) {
    return ((GenericRecord) record.get(META_KEY)).get(LAST_UPDATED_KEY).toString();
  }

  /**
   * This method identifies if the record is a deleted record. For a deleted record, the meta.tag
   * would be updated with a ActionType.REMOVE during the incremental parquet file creation, the
   * same information is being reused to check if the record is deleted or not.
   *
   * @param record
   * @return
   */
  private static Boolean isRecordDeleted(GenericRecord record) {
    Object tag = ((GenericRecord) record.get(META_KEY)).get(TAG_KEY);
    if (tag != null && tag instanceof Collection) {
      Collection tagCollection = (Collection) tag;
      if (!tagCollection.isEmpty()) {
        Iterator iterator = tagCollection.iterator();
        while (iterator.hasNext()) {
          GenericRecord tagCoding = (GenericRecord) iterator.next();
          if (tagCoding.get(SYSTEM_KEY) != null
              && tagCoding.get(SYSTEM_KEY).toString().equals(ActionType.REMOVE.getSystem())
              && tagCoding.get(CODE_KEY) != null
              && tagCoding.get(CODE_KEY).toString().equals(ActionType.REMOVE.toCode())) {
            return Boolean.TRUE;
          }
        }
      }
    }
    return Boolean.FALSE;
  }

  private static GenericRecord findLastRecord(
      Iterable<GenericRecord> iter1, Iterable<GenericRecord> iter2, Counter numDuplicates) {
    // Note we are assuming all times have the same time-zone to avoid parsing date values.
    String lastUpdated = null;
    GenericRecord lastRecord = null;
    int numRec = 0;
    for (GenericRecord record : iter1) {
      numRec++;
      String updateTimeStr = getUpdateTime(record);
      if (lastUpdated == null || lastUpdated.compareTo(updateTimeStr) < 0) {
        lastUpdated = updateTimeStr;
        lastRecord = record;
      }
    }
    for (GenericRecord record : iter2) {
      numRec++;
      String updateTimeStr = getUpdateTime(record);
      if (lastUpdated == null || lastUpdated.compareTo(updateTimeStr) < 0) {
        lastUpdated = updateTimeStr;
        lastRecord = record;
      }
    }
    if (numRec > 1) {
      numDuplicates.inc();
    }
    if (numRec > 2) {
      log.warn("Record with ID {} repeated more than twice!", lastRecord.get(ID_KEY));
    }
    return lastRecord;
  }

  static List<Pipeline> createMergerPipelines(ParquetMergerOptions options, FhirContext fhirContext)
      throws IOException {
    Preconditions.checkArgument(!options.getDwh1().isEmpty());
    Preconditions.checkArgument(!options.getDwh2().isEmpty());
    Preconditions.checkArgument(!options.getMergedDwh().isEmpty());

    Counter numOutputRecords =
        Metrics.counter(MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_OUTPUT_RECORDS);
    Counter numDuplicates =
        Metrics.counter(MetricsConstants.METRICS_NAMESPACE, MetricsConstants.NUM_DUPLICATES);

    String dwh1 = options.getDwh1();
    String dwh2 = options.getDwh2();
    String mergedDwh = options.getMergedDwh();
    DwhFiles dwhFiles1 = DwhFiles.forRoot(dwh1);
    DwhFiles dwhFiles2 = DwhFiles.forRoot(dwh2);
    DwhFiles mergedDwhFiles = DwhFiles.forRoot(mergedDwh);

    Set<String> resourceTypes1 = dwhFiles1.findNonEmptyFhirResourceTypes();
    Set<String> resourceTypes2 = dwhFiles2.findNonEmptyFhirResourceTypes();

    for (String resourceType : Sets.difference(resourceTypes1, resourceTypes2)) {
      dwhFiles1.copyResourcesToDwh(resourceType, mergedDwhFiles);
    }
    for (String resourceType : Sets.difference(resourceTypes2, resourceTypes1)) {
      dwhFiles2.copyResourcesToDwh(resourceType, mergedDwhFiles);
    }
    List<Pipeline> pipelines = new ArrayList<>();
    for (String type : resourceTypes1) {
      if (!resourceTypes2.contains(type)) {
        continue;
      }
      Pipeline pipeline = Pipeline.create(options);
      pipelines.add(pipeline);
      log.info("Merging resource type {}", type);
      PCollection<KV<String, GenericRecord>> firstKVs = readAndMapToId(pipeline, dwhFiles1, type);
      PCollection<KV<String, GenericRecord>> secondKVs = readAndMapToId(pipeline, dwhFiles2, type);
      final TupleTag<GenericRecord> tag1 = new TupleTag<>();
      final TupleTag<GenericRecord> tag2 = new TupleTag<>();
      PCollection<KV<String, CoGbkResult>> join =
          KeyedPCollectionTuple.of(tag1, firstKVs)
              .and(tag2, secondKVs)
              .apply(CoGroupByKey.create());
      PCollection<GenericRecord> merged =
          join.apply(
                  ParDo.of(
                      new DoFn<KV<String, CoGbkResult>, GenericRecord>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          KV<String, CoGbkResult> e = c.element();
                          Iterable<GenericRecord> iter1 = e.getValue().getAll(tag1);
                          Iterable<GenericRecord> iter2 = e.getValue().getAll(tag2);
                          GenericRecord lastRecord = findLastRecord(iter1, iter2, numDuplicates);
                          if (!isRecordDeleted(lastRecord)) {
                            numOutputRecords.inc();
                            c.output(lastRecord);
                          }
                        }
                      }))
              .setCoder(AvroCoder.of(ParquetUtil.getResourceSchema(type, fhirContext)));
      merged.apply(
          FileIO.<GenericRecord>write()
              .via(
                  ParquetIO.sink(ParquetUtil.getResourceSchema(type, fhirContext))
                      .withCompressionCodec(CompressionCodecName.SNAPPY))
              .to(mergedDwhFiles.getResourcePath(type).toString())
              .withSuffix(".parquet")
              // TODO if we don't set this, DirectRunner works fine but FlinkRunner only writes
              //   ~10% of the records. This is not specific to Parquet or GenericRecord; it even
              //   happens for TextIO. We should investigate this further and possibly file a bug.
              .withNumShards(options.getNumShards()));
    }
    return pipelines;
  }

  public static void main(String[] args) throws IOException {

    ParquetUtil.initializeAvroConverters();
    PipelineOptionsFactory.register(ParquetMergerOptions.class);
    ParquetMergerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ParquetMergerOptions.class);
    log.info("Flags: " + options);
    FhirContext fhirContext = FhirContexts.forR4();
    if (options.getDwh1().isEmpty()
        || options.getDwh2().isEmpty()
        || options.getMergedDwh().isEmpty()) {
      throw new IllegalArgumentException("All of --dwh1, --dwh2, and --mergedDwh should be set!");
    }

    List<Pipeline> pipelines = createMergerPipelines(options, fhirContext);
    EtlUtils.runMultipleMergerPipelinesWithTimestamp(pipelines, options);
    log.info("DONE!");
  }
}
