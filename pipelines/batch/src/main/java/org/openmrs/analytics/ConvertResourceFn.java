/*
 * Copyright 2020-2022 Google LLC
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
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertResourceFn extends FetchSearchPageFn<HapiRowDescriptor> {

  private static final Logger log = LoggerFactory.getLogger(ConvertResourceFn.class);

  private final SimpleDateFormat simpleDateFormat;

  private final HashMap<String, Counter> numFetchedResourcesMap;

  private final HashMap<String, Counter> totalParseTimeMillisMap;

  private final HashMap<String, Counter> totalGenerateTimeMillisMap;

  private final HashMap<String, Counter> totalPushTimeMillisMap;

  ConvertResourceFn(FhirEtlOptions options, String stageIdentifier) {
    super(options, stageIdentifier);
    this.numFetchedResourcesMap = new HashMap<String, Counter>();
    this.totalParseTimeMillisMap = new HashMap<String, Counter>();
    this.totalGenerateTimeMillisMap = new HashMap<String, Counter>();
    this.totalPushTimeMillisMap = new HashMap<String, Counter>();
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

    long startTime = System.currentTimeMillis();
    Resource resource = (Resource) parser.parseResource(jsonResource);
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
}
