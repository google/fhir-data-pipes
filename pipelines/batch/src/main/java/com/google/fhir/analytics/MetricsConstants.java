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

public class MetricsConstants {
  public static final String METRICS_NAMESPACE = "PipelineMetrics";
  public static final String NUM_MAPPED_RESOURCES = "numMappedResources_";
  public static final String NUM_FETCHED_RESOURCES = "numFetchedResources_";
  public static final String NUM_OUTPUT_RECORDS = "numOutputRecords";
  public static final String NUM_DUPLICATES = "numDuplicates";
  public static final String DATA_FORMAT_EXCEPTION_ERROR = "numDataFormatExceptionErrors_";
  public static final String TOTAL_FETCH_TIME_MILLIS = "totalFetchTimeMillis_";
  public static final String TOTAL_GENERATE_TIME_MILLIS = "totalGenerateTimeMillis_";
  public static final String TOTAL_PUSH_TIME_MILLIS = "totalPushTimeMillis_";
  public static final String TOTAL_PARSE_TIME_MILLIS = "totalParseTimeMillis_";
  public static final String TOTAL_NO_OF_RESOURCES = "totalNoOfResources_";
}
