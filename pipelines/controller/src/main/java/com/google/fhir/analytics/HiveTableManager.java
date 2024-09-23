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

import com.google.common.base.Strings;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class manages to create resources on Thrift Server post each pipeline run. */
public class HiveTableManager {

  private static final Logger logger = LoggerFactory.getLogger(HiveTableManager.class.getName());

  private final DataSource dataSource;

  private final String viewsDir;

  private static final String THRIFT_CONTAINER_PARQUET_DIR = "/dwh";

  public HiveTableManager(DatabaseConfiguration hiveDbConfig, String viewsDir) {
    // We don't expect many Hive queries hence choosing a fixed/low number of connections.
    // Also, we set the max number of connections equal to MIN_CONNECTIONS such that we never have
    // "excess" connections to be closed. This is to address this HiveDriver/Thrift-Server issue:
    // https://github.com/google/fhir-data-pipes/issues/483
    this.dataSource =
        JdbcConnectionPools.getInstance()
            .getPooledDataSource(
                JdbcConnectionPools.dbConfigToDataSourceConfig(hiveDbConfig),
                JdbcConnectionPools.MIN_CONNECTIONS);
    this.viewsDir = Strings.nullToEmpty(viewsDir);
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
        createViews(connection, resource);
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

    String location =
        String.format("%s/%s/%s", THRIFT_CONTAINER_PARQUET_DIR, thriftServerParquetPath, resource);
    String tableName = String.format("%s_%s", resource, timestamp);
    String sql =
        String.format(
            "CREATE TABLE IF NOT EXISTS default.%s USING PARQUET LOCATION '%s'",
            tableName, location);
    executeSql(connection, sql);

    // Instead of DROP and CREATE we use a VIEW for canonical tables such that the update happens
    // in one statement (because of lack of transactions in Hive JDBC driver). ALTER TABLE has its
    // own problems too, e.g., it does not seem to trigger parsing/changing schema.
    sql =
        String.format(
            "CREATE OR REPLACE VIEW default.%s AS SELECT * FROM default.%s", resource, tableName);
    executeSql(connection, sql);
  }

  /**
   * Creates the views registered in the `views/` directory for the given `resource`. Note since
   * these views might be user-provided, the SQLException is handled by logging an error but is not
   * thrown such that the error does not propagate.
   */
  private synchronized void createViews(Connection connection, String resource) {
    if (viewsDir.isEmpty()) {
      return;
    }
    List<Path> viewPaths = null;
    try {
      viewPaths =
          Files.list(Paths.get(viewsDir))
              .filter(
                  p ->
                      p.getFileName().toString().startsWith(resource)
                          && p.getFileName().toString().endsWith(".sql"))
              .collect(Collectors.toList());
    } catch (IOException e) {
      logger.error("Cannot get the list of files in {}", viewsDir, e);
      return;
    }
    if (viewPaths == null || viewPaths.isEmpty()) {
      logger.warn("No view files found for resource {} in {}", resource, viewsDir);
      return;
    }
    for (Path p : viewPaths) {
      try (BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
        String sql = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        executeSql(connection, sql);
      } catch (IOException | SQLException e) {
        logger.error("Error while executing SQL in {} :", p, e);
      }
    }
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
