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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.sql.SQLException;
import org.apache.beam.sdk.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Process FHIR resources in json/ndjson format from a FileIO.ReadableFile. */
public class ReadJsonFromFileFn extends ReadJsonFn<FileIO.ReadableFile> {

  private static final Logger log = LoggerFactory.getLogger(ReadJsonFromUrlFn.class);

  ReadJsonFromFileFn(FhirEtlOptions options, boolean isFileNdjson) {
    super(options, isFileNdjson);
  }

  @ProcessElement
  public void processElement(@Element FileIO.ReadableFile file)
      throws IOException, SQLException, ViewApplicationException, ProfileException {
    log.info("Reading file at {}", file.getMetadata());
    try (InputStream inputStream = Channels.newInputStream(file.open())) {
      processStream(inputStream);
    }
  }
}
