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

import ca.uhn.fhir.parser.DataFormatException;
import com.cerner.bunsen.exception.ProfileMapperException;
import com.google.common.collect.Sets;
import com.google.fhir.analytics.view.ViewApplicationException;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.Set;
import org.apache.beam.sdk.io.FileIO;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class reads the contents of a ndjson file and converts them into FHIR resources. */
public class ReadNDJsonFilesFn extends FetchSearchPageFn<FileIO.ReadableFile> {

  private static final Logger log = LoggerFactory.getLogger(ReadNDJsonFilesFn.class);

  private final Set<String> resourceTypes;

  ReadNDJsonFilesFn(FhirEtlOptions options) {
    super(options, "ReadNDJsonFilesFn");
    resourceTypes = Sets.newHashSet(options.getResourceList().split(","));
  }

  @Override
  public void setup() throws SQLException, PropertyVetoException, ProfileMapperException {
    super.setup();
    // Update the parser with the NDJsonParser. The NDJsonParser efficiently reads one record at a
    // time into memory and converts into a FHIR resource.
    parser = avroConversionUtil.getFhirContext().newNDJsonParser();
  }

  @ProcessElement
  public void processElement(@Element FileIO.ReadableFile file)
      throws IOException, SQLException, ViewApplicationException, ProfileMapperException {
    log.info("Reading file with metadata " + file.getMetadata());
    try {
      IBaseResource resource = parser.parseResource(Channels.newInputStream(file.open()));
      if (!"Bundle".equals(resource.fhirType())) {
        log.error(
            String.format(
                "The output type of the NDJsonParser should be a Bundle; type is %s, for file %s.",
                resource.fhirType(), file.getMetadata()));
      }
      Bundle bundle = (Bundle) resource;
      updateResolvedRefIds(bundle);
      processBundle(bundle, resourceTypes);
    } catch (DataFormatException | ClassCastException e) {
      log.error(String.format("Cannot parse content of file: %s", file.getMetadata()), e);
    }
  }
}
