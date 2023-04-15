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

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import java.io.IOException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a DoFn for fetching resource IDs for given search URLs. */
class FetchResourceIds extends DoFn<SearchSegmentDescriptor, String> {
  private static final Logger log = LoggerFactory.getLogger(FetchResourceIds.class);
  private final String stageIdentifier;
  protected FhirSearchUtil fhirSearchUtil;
  private FhirClientUtil fhirClientUtil;
  private final String sourceUrl;
  private final String sourceUser;
  private final String sourcePw;
  protected FhirContext fhirContext;
  private final Counter totalIdFetchTimeMillis;

  FetchResourceIds(FhirEtlOptions options, String stageIdentifier) {
    this.stageIdentifier = stageIdentifier;
    this.totalIdFetchTimeMillis =
        Metrics.counter(EtlUtils.METRICS_NAMESPACE, "totalIdFetchTimeMillis_" + stageIdentifier);
    this.sourceUrl = options.getFhirServerUrl();
    this.sourceUser = options.getFhirServerUserName();
    this.sourcePw = options.getFhirServerPassword();
  }

  @Setup
  public void setup() {
    fhirContext = FhirContexts.forR4();
    fhirClientUtil = new FhirClientUtil(sourceUrl, sourceUser, sourcePw, fhirContext);
    fhirSearchUtil = new FhirSearchUtil(fhirClientUtil);
  }

  @ProcessElement
  public void processElement(@Element SearchSegmentDescriptor segment, OutputReceiver<String> out)
      throws IOException {
    String searchUrl = segment.searchUrl();
    log.info(
        String.format(
            "Fetching %d resource IDs for stage %s; URL= %s",
            segment.count(),
            this.stageIdentifier,
            searchUrl.substring(0, Math.min(200, searchUrl.length()))));
    long fetchStartTime = System.currentTimeMillis();
    Bundle bundle = fhirSearchUtil.searchByUrl(searchUrl, segment.count(), null);
    totalIdFetchTimeMillis.inc(System.currentTimeMillis() - fetchStartTime);
    if (bundle == null || bundle.getEntry() == null) {
      log.error("This search returned no results: {}", searchUrl);
      return;
    }
    for (BundleEntryComponent entry : bundle.getEntry()) {
      Resource resource = entry.getResource();
      out.output(resource.getIdPart());
    }
  }
}
