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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.fhir.analytics.converter.JsonDateDeserializer;
import com.google.fhir.analytics.converter.JsonDateSerializer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/** The response body of the bulk export job status */
@Data
@Builder
public class BulkExportResponse {

  @JsonSerialize(using = JsonDateSerializer.class)
  @JsonDeserialize(using = JsonDateDeserializer.class)
  private Date transactionTime;

  private String request;

  private boolean requiresAccessToken;

  private List<Output> output;

  private List<Output> deleted;

  private List<Output> error;

  private Map<String, Object> extension;

  @Data
  @Builder
  public static class Output {

    private String type;

    private String url;
  }
}
