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
package com.google.fhir.analytics.model;

import com.google.fhir.analytics.converter.JsonDateCodec;
import com.google.gson.annotations.JsonAdapter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Builder;

/** The response body of the bulk export job status */
@Builder
public record BulkExportResponse(
    @JsonAdapter(JsonDateCodec.class) Date transactionTime,
    String request,
    boolean requiresAccessToken,
    List<Output> output,
    List<Output> deleted,
    List<Output> error,
    Map<String, Object> extension) {

  @Builder
  public record Output(String type, String url) {}
}
