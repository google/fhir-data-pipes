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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchPatients extends PTransform<PCollection<KV<String, Integer>>, PDone> {

  private static final Logger log = LoggerFactory.getLogger(FetchPatientHistory.class);

  private static final String PATIENT = "Patient";

  private final FetchSearchPageFn<KV<String, Integer>> fetchSearchPageFn;

  private final Schema schema;

  FetchPatients(FhirEtlOptions options, Schema schema) {
    Preconditions.checkState(!options.getActivePeriod().isEmpty());
    List<String> dateRange = FhirSearchUtil.getDateRange(options.getActivePeriod());

    int count = options.getBatchSize();
    this.schema = schema;

    fetchSearchPageFn =
        new FetchSearchPageFn<KV<String, Integer>>(options, "PatientById") {

          @ProcessElement
          public void processElement(@Element KV<String, Integer> patientIdCount)
              throws IOException, SQLException {
            String patientId = patientIdCount.getKey();
            log.info(
                String.format(
                    "Already fetched %d resources for patient %s",
                    patientIdCount.getValue(), patientId));
            // TODO use openmrsUtil.fetchResource() instead of search and process bundle.
            Bundle bundle =
                this.fhirSearchUtil.searchByUrl(
                    PATIENT + "?_id=" + patientId, count, SummaryEnum.DATA);
            processBundle(bundle);
          }
        };
  }

  @Override
  public PDone expand(PCollection<KV<String, Integer>> patientIdCounts) {
    patientIdCounts.apply(ParDo.of(fetchSearchPageFn));
    return PDone.in(patientIdCounts.getPipeline());
  }
}
