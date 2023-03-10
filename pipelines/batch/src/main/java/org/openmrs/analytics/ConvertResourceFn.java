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
package org.openmrs.analytics;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.codesystems.ActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertResourceFn extends FetchSearchPageFn<HapiRowDescriptor> {

  private static final Logger log = LoggerFactory.getLogger(ConvertResourceFn.class);

  private final SimpleDateFormat simpleDateFormat;

  private final HashMap<String, Counter> numFetchedResourcesMap;

  private final HashMap<String, Counter> totalParseTimeMillisMap;

  private final HashMap<String, Counter> totalGenerateTimeMillisMap;

  private final HashMap<String, Counter> totalPushTimeMillisMap;

  private final Boolean isRunIncremental;

  ConvertResourceFn(FhirEtlOptions options, String stageIdentifier) {
    super(options, stageIdentifier);
    this.numFetchedResourcesMap = new HashMap<String, Counter>();
    this.totalParseTimeMillisMap = new HashMap<String, Counter>();
    this.totalGenerateTimeMillisMap = new HashMap<String, Counter>();
    this.totalPushTimeMillisMap = new HashMap<String, Counter>();
    this.isRunIncremental = options.isRunIncremental();
    List<String> resourceTypes = Arrays.asList(options.getResourceList().split(","));
    for (String resourceType : resourceTypes) {
      this.numFetchedResourcesMap.put(
          resourceType,
          Metrics.counter(EtlUtils.METRICS_NAMESPACE, "numFetchedResources_" + resourceType));
      this.totalParseTimeMillisMap.put(
          resourceType,
          Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalParseTimeMillis_" + resourceType));
      this.totalGenerateTimeMillisMap.put(
          resourceType,
          Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalGenerateTimeMillis_" + resourceType));
      this.totalPushTimeMillisMap.put(
          resourceType,
          Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalPushTimeMillis_" + resourceType));
    }
    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
  }

  public void writeResource(HapiRowDescriptor element)
      throws IOException, ParseException, SQLException {
    String resourceId = element.resourceId();
    String resourceType = element.resourceType();
    Meta meta =
        new Meta()
            .setVersionId(element.resourceVersion())
            .setLastUpdated(simpleDateFormat.parse(element.lastUpdated()));
    String jsonResource = element.jsonResource();
    // The jsonResource field will be empty in case of deleted records and are skipped when written
    // to target parquet files/sinkDb. This is fine for initial batch as they need not be migrated,
    // but for incremental run they need to be migrated and the original records have to be deleted
    // from the consolidated parquet files/sinkDb.
    // TODO https://github.com/google/fhir-data-pipes/issues/547
    long startTime = System.currentTimeMillis();
    Resource resource = null;
    if (jsonResource == null || jsonResource.isBlank()) {
      // Ignore deleted records during initial batch run
      if (!isRunIncremental) {
        return;
      }

      // Create a new resource and attach the meta with a deleted tag, this deleted tag is later
      // used in the merge process to identify if the record has been deleted
      resource = createNewFhirResource(resourceType);
      ActionType removeAction = ActionType.REMOVE;
      meta.setLastUpdated(new Date());
      meta.addTag(
          new Coding(removeAction.getSystem(), removeAction.toCode(), removeAction.getDisplay()));
    } else {
      resource = (Resource) parser.parseResource(jsonResource);
    }
    totalParseTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    resource.setId(resourceId);
    resource.setMeta(meta);

    numFetchedResourcesMap.get(resourceType).inc(1);

    if (!parquetFile.isEmpty()) {
      startTime = System.currentTimeMillis();
      parquetUtil.write(resource);
      totalGenerateTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    }
    if (!this.sinkPath.isEmpty()) {
      startTime = System.currentTimeMillis();
      fhirStoreUtil.uploadResource(resource);
      totalPushTimeMillisMap.get(resourceType).inc(System.currentTimeMillis() - startTime);
    }
    if (!this.sinkDbUrl.isEmpty()) {
      jdbcWriter.writeResource(resource);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext processContext)
      throws IOException, ParseException, SQLException {
    HapiRowDescriptor element = processContext.element();
    writeResource(element);
  }

  private Resource createNewFhirResource(String resourceType) {
    try {
      return (Resource)
          Class.forName("org.hl7.fhir.r4.model." + resourceType).getConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      String errorMessage =
          String.format("Failed to instantiate new FHIR resource of type %s", resourceType);
      log.error(errorMessage, e);
      throw new FHIRException(errorMessage, e);
    }
  }
}
