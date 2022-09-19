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

import ca.uhn.fhir.parser.DataFormatException;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.hl7.fhir.instance.model.api.IBaseReference;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadJsonFilesFn extends FetchSearchPageFn<FileIO.ReadableFile> {

  private static final Logger log = LoggerFactory.getLogger(ReadJsonFilesFn.class);

  private final Set<String> resourceTypes;

  ReadJsonFilesFn(FhirEtlOptions options) {
    super(options, "ReadJsonFiles");
    resourceTypes = Sets.newHashSet(options.getResourceList().split(","));
  }

  @ProcessElement
  public void processElement(
      @Element FileIO.ReadableFile file, OutputReceiver<KV<String, Integer>> out)
      throws IOException, SQLException {
    log.info("Reading file with metadata " + file.getMetadata());
    String fileContent = file.readFullyAsUTF8String();
    try {
      IBaseResource resource = parser.parseResource(fileContent);
      if (!"Bundle".equals(resource.fhirType())) {
        log.error(
            String.format(
                "The content of file %s is not a Bundle; type is %s.",
                file.getMetadata().toString(), resource.fhirType()));
      }
      Bundle bundle = (Bundle) resource;
      updateResolvedRefIds(bundle);
      processBundle(bundle, resourceTypes);
    } catch (DataFormatException | ClassCastException e) {
      log.error("Cannot parse content of file " + file.getMetadata().toString() + e);
    }
  }

  /**
   * For every URN reference whose target is found in this bundle, updates the reference to the
   * relative URL of the found resource. This is needed when original references are not URLs
   * (relative or absolute) and instead are URNs, e.g., `urn:uuid:...`. During parsing, these
   * references are resolved properly, i.e., if the referenced resource is in the Bundle, it is
   * found. However, the references are kept as original URNs. With this function we are trying to
   * simulate the logic of uploading the Bundle to a FHIR server and then downloading those
   * resources. The logic implemented here is based on how HAPI parser resolves the same references
   * within a Bundle; see {@code ca.uhn.fhir.parser.ParserState.stitchBundleCrossReferences}.
   *
   * @param bundle the bundle whose references are updated.
   */
  private void updateResolvedRefIds(Bundle bundle) {
    for (BundleEntryComponent entry : bundle.getEntry()) {
      List<IBaseReference> refs =
          fhirContext
              .newTerser()
              .getAllPopulatedChildElementsOfType(entry.getResource(), IBaseReference.class);
      for (IBaseReference ref : refs) {
        IBaseResource resource = ref.getResource();
        if (resource != null) {
          if (ref.getReferenceElement() == null
              || ref.getReferenceElement().getIdPart() == null
              || ref.getReferenceElement().getIdPart().startsWith("urn:")) {
            log.debug(
                String.format(
                    "Updating URN reference %s to the resolved resource ID %s",
                    ref.getReferenceElement(), resource.getIdElement()));
            ref.setReference(resource.getIdElement().getValue());
          }
        }
      }
    }
  }
}
