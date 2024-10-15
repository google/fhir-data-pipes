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

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

@DefaultCoder(SerializableCoder.class)
@Data
@Builder
public class MdmLink implements Serializable {

  private static final long serialVersionUID = 1L;

  // the resource that contains references to golden resources, e.g. Encounter
  String resourceId;

  // the resource that is the golden resource, e.g. Patient
  String sourceFhirId;
  String goldenFhirId;

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MdmLink)) {
      return false;
    }

    MdmLink mdmLink = (MdmLink) other;

    return sourceFhirId.equals(mdmLink.sourceFhirId)
        && goldenFhirId.equals(mdmLink.goldenFhirId)
        && resourceId.equals(mdmLink.resourceId);
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + (sourceFhirId == null ? 0 : sourceFhirId.hashCode());
    hash = 31 * hash + (goldenFhirId == null ? 0 : goldenFhirId.hashCode());
    hash = 31 * hash + (resourceId == null ? 0 : resourceId.hashCode());
    return hash;
  }
}
