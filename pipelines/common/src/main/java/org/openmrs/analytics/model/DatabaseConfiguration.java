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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabaseConfiguration {

  private LinkedHashMap<String, EventConfiguration> eventConfigurations;

  private LinkedHashMap<String, String> debeziumConfigurations;

  // Accessor functions for any configuration parameter that needs to be exposed beyond Debezium.
  // For Debezium only configs, it is okay to directly use the string values which is done mostly
  // in `DebeziumListener.getDebeziumConfig`.

  public String getDbUser() {
    return debeziumConfigurations.get("databaseUser");
  }

  public String getDbPassword() {
    return debeziumConfigurations.get("databasePassword");
  }

  public String getDbPort() {
    return debeziumConfigurations.get("databasePort");
  }

  public String getDbHostName() {
    return debeziumConfigurations.get("databaseHostName");
  }

  public String getDbName() {
    return debeziumConfigurations.get("databaseName");
  }

  public String getDbService() {
    return debeziumConfigurations.get("databaseService");
  }

  /**
   * From the config parameters, this reconstructs a JDBC URL.
   *
   * @return the JDBC URL, e.g., "jdbc:mysql://localhost:3306/openmrs".
   */
  public String makeJdbsUrlFromConfig() {
    Preconditions.checkNotNull(getDbHostName());
    Preconditions.checkNotNull(getDbPort());
    Preconditions.checkNotNull(getDbName());
    return String.format(
        "jdbc:%s://%s:%s/%s", getDbService(), getDbHostName(), getDbPort(), getDbName());
  }

  /**
   * This is a factory method for creating instances of this class from the content of a JSON file.
   *
   * @param fileName the name of the file with JSON content that resembles `GeneralConfiguration`.
   * @return the created instance
   * @throws IOException in case of any IO errors.
   */
  public static DatabaseConfiguration createConfigFromFile(String fileName) throws IOException {
    Gson gson = new Gson();
    Path pathToFile = Paths.get(fileName);
    try (Reader reader = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
      return gson.fromJson(reader, DatabaseConfiguration.class);
    }
  }
}
