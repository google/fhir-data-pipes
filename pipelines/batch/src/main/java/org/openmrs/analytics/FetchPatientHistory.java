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

import ca.uhn.fhir.rest.api.SummaryEnum;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchPatientHistory extends PTransform<PCollection<KV<String, Integer>>, PDone> {

  private static final Logger log = LoggerFactory.getLogger(FetchPatientHistory.class);

  private final FetchSearchPageFn<KV<String, Integer>> fetchSearchPageFn;

  private final String startDate;

  FetchPatientHistory(FhirEtlOptions options, String resourceType) {
    List<String> dateRange = FhirSearchUtil.getDateRange(options.getActivePeriod());
    final String stageId = resourceType + "_history";
    if (dateRange.isEmpty() || dateRange.get(0).isEmpty()) {
      startDate = "";
      log.info("Empty start of active period; skipping step " + stageId);
    } else {
      startDate = dateRange.get(0);
      log.info("The last date for fetching patient history is " + startDate);
    }

    int count = options.getBatchSize();

    fetchSearchPageFn =
        new FetchSearchPageFn<KV<String, Integer>>(options, stageId) {

          @ProcessElement
          public void ProcessElement(@Element KV<String, Integer> patientIdCount)
              throws IOException, SQLException {
            if (startDate.isEmpty()) {
              return;
            }
            String patientId = patientIdCount.getKey();
            log.info(
                String.format(
                    "Fetching historical %s resources for patient  %s", resourceType, patientId));
            Bundle bundle =
                this.fhirSearchUtil.searchByPatientAndLastDate(
                    resourceType, patientId, startDate, count);
            processBundle(bundle);
            String nextUrl = this.fhirSearchUtil.getNextUrl(bundle);
            while (nextUrl != null) {
              bundle = this.fhirSearchUtil.searchByUrl(nextUrl, count, SummaryEnum.DATA);
              processBundle(bundle);
              nextUrl = this.fhirSearchUtil.getNextUrl(bundle);
            }
          }
        };
  }

  @Override
  public PDone expand(PCollection<KV<String, Integer>> patientIdCounts) {
    patientIdCounts.apply(ParDo.of(fetchSearchPageFn));
    return PDone.in(patientIdCounts.getPipeline());
  }
}
