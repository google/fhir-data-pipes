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
package com.google.fhir.analytics;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Coding;

@DefaultCoder(SerializableCoder.class)
@Data
@Builder
public class ResourceTag implements Serializable {

  private static final int prime = 1000003;

  private static final long serialVersionUID = 1L;

  Coding coding;
  String resourceId;
  Integer tagType;

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof ResourceTag)) {
      return false;
    }

    ResourceTag resourceTag = (ResourceTag) other;

    return coding != null
        && coding.equalsDeep(resourceTag.coding)
        && StringUtils.compare(resourceId, resourceTag.getResourceId()) == 0
        && ObjectUtils.compare(tagType, resourceTag.getTagType()) == 0;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash *= prime;
    hash ^= coding.hashCode();
    hash *= prime;
    hash ^= resourceId.hashCode();
    hash *= prime;
    hash ^= tagType.hashCode();
    return hash;
  }
}
