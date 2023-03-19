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

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.hl7.fhir.r4.model.Coding;

@DefaultCoder(SerializableCoder.class)
@Data
@Builder
public class ResourceTag implements Serializable {

  private static final long serialVersionUID = 1L;

  Coding coding;
  String resourceId;
  Integer tagType;
}
