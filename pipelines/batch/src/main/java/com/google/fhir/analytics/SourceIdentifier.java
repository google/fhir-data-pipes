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
public class SourceIdentifier implements Serializable {

  private static final long serialVersionUID = 1L;

  String resourceId;
  String system;
  String value;

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof SourceIdentifier)) {
      return false;
    }

    SourceIdentifier sourceIdentifier = (SourceIdentifier) other;

    return resourceId.equals(sourceIdentifier.resourceId)
        && system.equals(sourceIdentifier.system)
        && value.equals(sourceIdentifier.value);
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = 31 * hash + (resourceId == null ? 0 : resourceId.hashCode());
    hash = 31 * hash + (system == null ? 0 : system.hashCode());
    hash = 31 * hash + (value == null ? 0 : value.hashCode());
    return hash;
  }
}
