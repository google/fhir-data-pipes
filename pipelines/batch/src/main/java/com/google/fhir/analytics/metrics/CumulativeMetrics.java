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
package com.google.fhir.analytics.metrics;

public final class CumulativeMetrics {
  private final long totalResources;
  private final long fetchedResources;
  private final long mappedResources;

  public CumulativeMetrics(long totalResources, long fetchedResources, long mappedResources) {
    this.totalResources = totalResources;
    this.fetchedResources = fetchedResources;
    this.mappedResources = mappedResources;
  }

  public long getTotalResources() {
    return totalResources;
  }

  public long getFetchedResources() {
    return fetchedResources;
  }

  public long getMappedResources() {
    return mappedResources;
  }
}
