/*
 * Copyright 2020-2022 Google LLC
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
package org.openmrs.analytics.model;

import java.util.LinkedHashMap;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EventConfiguration {
  private boolean enabled;
  private String title;
  private String parentForeignKey;
  private String childPrimaryKey;
  private String parentTable;
  private LinkedHashMap<String, String> linkTemplates;
  private String databaseHostName;
  private int databasePort;
  private String databaseUser;
  private String databasePassword;
  private int databaseServerId;
  private String databaseServerName;
  private String databaseSchema;
  private String databaseOffsetStorage;
  private String databaseHistory;
  private String snapshotMode;
}
