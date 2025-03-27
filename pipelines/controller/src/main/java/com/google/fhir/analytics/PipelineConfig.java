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

import lombok.Builder;
import lombok.Data;

/**
 * This is a wrapper class on FhirEtlOptions to cater to extra config params e.g. params needed for
 * Hive table creation.
 */
@Data
@Builder(toBuilder = true)
public class PipelineConfig {
  private FhirEtlOptions fhirEtlOptions;
  // This is the path relative to the base dir for all DWH roots. For example, if the dwhRootPrefix
  // is `/a/b/c/prefix` and the actual path of a DWH is `/a/b/c/prefix_TIMESTAMP_1`, then
  // `thriftServerParquetPath` should be `prefix_TIMESTAMP_1`. The reason for this name is that
  // this is the path under `/dwh/` on the Thrift server docker image in the standard configuration
  // where the common base dir for all DWHs is mounted on `/dwh/`.
  private String thriftServerParquetPath;
  private String timestampSuffix;
}
