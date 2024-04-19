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

import ca.uhn.fhir.context.FhirVersionEnum;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Base common class for the batch and incremental pipeline runs, containing the common
 * configurations
 */
public interface BasePipelineOptions extends PipelineOptions {
  @Description(
      "The approximate size (bytes) of the row-groups in Parquet files. When this size is reached,"
          + " the content is flushed to disk. A large value means more data for one column can fit"
          + " into one big column chunk which means better compression and faster IO/query. On the"
          + " downside, larger value means more in-memory size will be needed to hold the data "
          + " before writing to files. The default value of 0 means use the default row-group size"
          + " of Parquet writers.")
  @Default.Integer(0)
  int getRowGroupSizeForParquetFiles();

  void setRowGroupSizeForParquetFiles(int value);

  @Description(
      "Directory containing the structure definition files for any custom profiles that needs to be"
          + " supported. If it starts with `classpath:` then the classpath is searched; and the"
          + " path should always start with `/`. Do not use this if custom profiles are not needed."
          + " Example: `classpath:/r4-us-core-definitions` is the classpath name under the"
          + " resources folder of module `extension-structure-definitions`.")
  @Default.String("")
  String getStructureDefinitionsPath();

  void setStructureDefinitionsPath(String value);

  @Description("The fhir version to be used for the FHIR Context APIs")
  @Default.Enum("R4")
  FhirVersionEnum getFhirVersion();

  void setFhirVersion(FhirVersionEnum fhirVersionEnum);

  @Description(
      "The maximum depth for traversing StructureDefinitions in Parquet schema generation."
          + " Please note in most cases, the default 1 is sufficient and increasing that can"
          + " result in significantly larger schema and more complexity. For details see:"
          + " https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#recursive-structures")
  @Default.Integer(1)
  Integer getRecursiveDepth();

  void setRecursiveDepth(Integer value);

  @Description("Whether to generate the full ID (including type) or only the logical part.")
  @Default.Boolean(false)
  Boolean getFullId();

  void setFullId(Boolean value);
}
