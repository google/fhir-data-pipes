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

import com.cerner.bunsen.exception.ProfileException;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessGenericRecords extends FetchSearchPageFn<GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(ProcessGenericRecords.class);

  // This is the number of resources that are put in each bundle to be processed. The main impact
  // of this is when we are uploading to another FHIR server.
  // This should probably be exposed as a config param, but for now this is a good enough constant.
  private static final int BUNDLE_SIZE = 10;

  private String resourceType;
  private List<Resource> cachedResources;
  private Counter totalAvroConversionTime;
  private Counter totalAvroConversions;

  ProcessGenericRecords(FhirEtlOptions options, String resourceType) {
    super(options, "ProcessGenericRecords_" + resourceType);
    this.resourceType = resourceType;
  }

  @Override
  public void setup() throws SQLException, ProfileException {
    super.setup();
    cachedResources = new ArrayList<>();
    totalAvroConversionTime =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.TOTAL_AVRO_CONVERSION_TIME_MILLIS + resourceType);
    totalAvroConversions =
        Metrics.counter(
            MetricsConstants.METRICS_NAMESPACE,
            MetricsConstants.TOTAL_AVRO_CONVERSIONS + resourceType);
  }

  @Override
  public void teardown() throws IOException {
    if (!cachedResources.isEmpty()) {
      try {
        processBundle(flushCachToBundle());
      } catch (SQLException | ViewApplicationException | ProfileException e) {
        // This is not perfect but the parent teardown only has IOException.
        log.error("Caught exception in teardown: ", e);
        throw new IOException(e);
      }
    }
    super.teardown();
  }

  private Bundle flushCachToBundle() {
    Bundle bundle = new Bundle();
    for (Resource resource : cachedResources) {
      bundle.addEntry(new BundleEntryComponent().setResource(resource));
    }
    cachedResources.clear();
    return bundle;
  }

  @ProcessElement
  public void processElement(@Element GenericRecord record)
      throws IOException, SQLException, ViewApplicationException, ProfileException {
    try {
      long startTime = System.currentTimeMillis();
      Resource resource = avroConversionUtil.convertToHapi(record, resourceType);
      totalAvroConversionTime.inc(System.currentTimeMillis() - startTime);
      totalAvroConversions.inc();
      cachedResources.add(resource);
      if (cachedResources.size() >= BUNDLE_SIZE) {
        processBundle(flushCachToBundle());
      }
    } catch (IllegalArgumentException e) {
      log.error("Dropping bad record because: " + e.getMessage());
    }
  }
}
