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

/**
 * Class containing the list of different modes via which the application can fetch FHIR resources
 * from the source.
 */
public enum FhirFetchMode {
  BULK_EXPORT,
  FHIR_SEARCH,
  HAPI_JDBC,
  OPENMRS_JDBC,
  PARQUET,
  JSON,
  NDJSON
}
