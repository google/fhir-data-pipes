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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class manages to create resources on Thrift Server post each pipeline run. */
public class ThriftServerHiveResourceManager {

  private static final Logger logger =
      LoggerFactory.getLogger(ThriftServerHiveResourceManager.class.getName());
  private final String jdbcDriverClass;
  private final String jdbcUrl;
  private final String user;
  private final String password;

  public ThriftServerHiveResourceManager(
      String jdbcDriverClass, String jdbcUrl, String user, String password) {
    this.jdbcDriverClass = jdbcDriverClass;
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
  }

  /**
   * Method to create resources on Thrift Server Hive.
   *
   * @param resourceList Comma separated list of resources such as Patient, Observation, Encounter
   * @param timestamp Timestamp suffix to be used in table name.
   * @param thriftServerParquetPath location of parquet files in Thrift Server
   * @throws SQLException
   */
  public void createResources(String resourceList, String timestamp, String thriftServerParquetPath)
      throws SQLException {
    try {
      Class.forName(jdbcDriverClass);
    } catch (ClassNotFoundException e) {
      logger.error("Unable to locate Hive JDBC driver.");
      return;
    }
    if (resourceList == null || resourceList.isEmpty()) {
      return;
    }
    String[] resources = resourceList.split(",");
    if (resources == null || resources.length == 0) {
      return;
    }
    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      for (String resource : resources) {
        createResource(connection, resource, timestamp, thriftServerParquetPath);
      }
    }
  }

  /**
   * This method will create table 'encounter_2023_01_24t18_42_54_302111z' if the given resource is
   * Encounter and the timestamp suffix is 2023_01_24t18_42_54_302111z
   *
   * <p>wrt PARQUET LOCATION, /dwh is symlink for mounted volume defined in docker-compose and
   * thriftServerParquetPath would be controller_DWH_ORIG_TIMESTAMP_2023_01_24T18_42_54_302111Z
   */
  private void createResource(
      Connection connection, String resource, String timestamp, String thriftServerParquetPath)
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String sql =
          String.format(
              "CREATE TABLE IF NOT EXISTS default.%s_%s USING PARQUET LOCATION '/dwh/%s/%s'",
              resource, timestamp, thriftServerParquetPath, resource);
      statement.execute(sql);
    }
  }
}
