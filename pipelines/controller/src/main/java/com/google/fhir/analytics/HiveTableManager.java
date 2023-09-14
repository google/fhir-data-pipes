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

import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class manages to create resources on Thrift Server post each pipeline run. */
public class HiveTableManager {

  private static final Logger logger = LoggerFactory.getLogger(HiveTableManager.class.getName());

  // We don't expect many Hive queries hence choosing a fixed/low number of connections.
  private static final int CONNECTION_POOL_SIZE = 3;

  private final DataSource dataSource;

  private static final String THRIFT_CONTAINER_PARQUET_DIR = "/dwh";

  public HiveTableManager(DatabaseConfiguration hiveDbConfig) throws PropertyVetoException {
    this.dataSource =
        JdbcConnectionPools.getInstance()
            .getPooledDataSource(
                JdbcConnectionPools.dbConfigToDataSourceConfig(hiveDbConfig),
                CONNECTION_POOL_SIZE,
                CONNECTION_POOL_SIZE);
  }

  /**
   * Method to create tables on [Thrift] Hive server. This creates a timestamped table for each
   * resource and also updates the "canonical table", i.e., the table name with no timestamp, to
   * point to the given set of files.
   *
   * @param resources list of resources such as Patient, Observation, and Encounter; the directories
   *     corresponding to these resources are assumed to exist and have valid Parquet files.
   * @param timestamp Timestamp suffix to be used in table name.
   * @param thriftServerParquetPath location of parquet files in Thrift Server; this is relative to
   *     the THRIFT_CONTAINER_PARQUET_DIR directory.
   * @throws SQLException
   */
  public synchronized void createResourceAndCanonicalTables(
      List<String> resources, String timestamp, String thriftServerParquetPath)
      throws SQLException {
    if (resources == null || resources.isEmpty()) {
      return;
    }

    try (Connection connection = dataSource.getConnection()) {
      for (String resource : resources) {
        createTablesForResource(connection, resource, timestamp, thriftServerParquetPath);
      }
    }
  }

  /**
   * This method will create table 'encounter_2023_01_24t18_42_54_302111z' if the given resource is
   * Encounter and the timestamp suffix is 2023_01_24t18_42_54_302111z
   *
   * <p>wrt PARQUET LOCATION, THRIFT_CONTAINER_PARQUET_DIR is the directory hosting parquet files,
   * thriftServerParquetPath is the exact path for parquet files and resource shall be the
   * respective resource name e.g. Patient
   */
  private synchronized void createTablesForResource(
      Connection connection, String resource, String timestamp, String thriftServerParquetPath)
      throws SQLException {

    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS default.%s_%s USING PARQUET LOCATION '%s/%s/%s'",
            resource, timestamp, THRIFT_CONTAINER_PARQUET_DIR, thriftServerParquetPath, resource);
    executeSql(connection, sql);

    // Drop canonical table if exists.
    sql = String.format("DROP TABLE IF EXISTS default.%s", resource);
    executeSql(connection, sql);

    // Create canonical table with latest parquet files.
    sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS default.%s USING PARQUET LOCATION '%s/%s/%s'",
            resource, THRIFT_CONTAINER_PARQUET_DIR, thriftServerParquetPath, resource);
    executeSql(connection, sql);
  }

  public void showTables() throws SQLException {
    logger.info("List of Hive tables:");
    String sql = "SHOW TABLES;";
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      if (resultSet != null && resultSet.next()) {
        do {
          logger.info(resultSet.getString("namespace") + '.' + resultSet.getString("tableName"));
        } while (resultSet.next());
      }
    }
  }

  private void executeSql(Connection connection, String sql) throws SQLException {
    logger.info("Executing SQL query: {}", sql);
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
