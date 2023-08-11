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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.fhir.analytics.JdbcConnectionPools.DataSourceConfig;
import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.hl7.fhir.r4.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO switch to using JdbcIO; currently we can't do this because resource processing is done
// in the same DoFn as writing (for historical memory-related reasons).
// See: https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/288

/** Writes FHIR resources to a relational database with a simple JSON based table schema. */
public class JdbcResourceWriter {

  private static final Logger log = LoggerFactory.getLogger(JdbcResourceWriter.class);

  private final String tablePrefix;

  private final boolean useSingleTable;

  private IParser parser;

  private DataSource jdbcDataSource;

  JdbcResourceWriter(
      DataSource jdbcDataSource,
      String tablePrefix,
      boolean useSingleTable,
      FhirContext fhirContext) {
    this.tablePrefix = tablePrefix;
    this.useSingleTable = useSingleTable;
    this.parser = fhirContext.newJsonParser();
    this.jdbcDataSource = jdbcDataSource;
  }

  private static void createSingleTable(DataSource dataSource, String createStatement)
      throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(createStatement)) {
      log.info("Table creation statement is " + statement);
      statement.execute();
    }
  }

  static void createTables(FhirEtlOptions options)
      throws PropertyVetoException, IOException, SQLException {
    // This should not be triggered in pipeline workers because concurrent CREATEs lead to failures:
    // https://stackoverflow.com/questions/54351783/duplicate-key-value-violates-unique-constraint
    Preconditions.checkArgument(!Strings.nullToEmpty(options.getSinkDbConfigPath()).isEmpty());
    Preconditions.checkArgument(
        !options.getSinkDbTablePrefix().isEmpty() || !options.getUseSingleSinkTable());
    DataSourceConfig dbConfig =
        JdbcConnectionPools.dbConfigToDataSourceConfig(
            DatabaseConfiguration.createConfigFromFile(options.getSinkDbConfigPath()));
    DataSource jdbcSource =
        JdbcConnectionPools.getInstance()
            .getPooledDataSource(
                dbConfig, options.getJdbcInitialPoolSize(), options.getJdbcMaxPoolSize());
    log.info(
        String.format(
            "Connecting to DB url %s with user %s.", dbConfig.jdbcUrl(), dbConfig.dbUser()));
    if (options.getUseSingleSinkTable()) {
      // For CREATE statements we cannot (and don't need to) use a placeholder for table name, i.e.,
      // we don't need to use PreparedStatement with '?'.
      String tableCreate =
          "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(100) NOT NULL, "
              + "type VARCHAR(50) NOT NULL, datab JSONB, PRIMARY KEY (id, type) );";
      String createStatement = String.format(tableCreate, options.getSinkDbTablePrefix());
      createSingleTable(jdbcSource, createStatement);
    } else {
      for (String resourceType : options.getResourceList().split(",")) {
        String tableCreate =
            "CREATE TABLE IF NOT EXISTS %s (id VARCHAR(100) NOT NULL, "
                + "datab JSONB, PRIMARY KEY (id) );";
        String createStatement =
            String.format(tableCreate, options.getSinkDbTablePrefix() + resourceType);
        createSingleTable(jdbcSource, createStatement);
      }
    }
  }

  public void writeResource(Resource resource) throws SQLException {
    // TODO add the option for SQL-on-FHIR schema
    try (Connection connection = jdbcDataSource.getConnection()) {
      String tableName = tablePrefix + resource.getResourceType().name();
      PreparedStatement statement = null;
      if (useSingleTable) {
        tableName = tablePrefix;
        statement =
            connection.prepareStatement(
                "INSERT INTO "
                    + tableName
                    + " (id, type, datab) VALUES(?, ?, ?::jsonb) "
                    + "ON CONFLICT (id, type) DO UPDATE SET id=?, type=?, datab=?::jsonb ;");
      } else {
        statement =
            connection.prepareStatement(
                "INSERT INTO "
                    + tableName
                    + " (id, datab) VALUES(?, ?::jsonb) "
                    + "ON CONFLICT (id) DO UPDATE SET id=?, datab=?::jsonb ;");
      }
      int paramInd = 1;
      statement.setString(paramInd, resource.getIdElement().getIdPart());
      if (useSingleTable) {
        paramInd++;
        statement.setString(paramInd, resource.getResourceType().name());
      }
      paramInd++;
      statement.setString(paramInd, parser.encodeResourceToString(resource));
      paramInd++;
      statement.setString(paramInd, resource.getIdElement().getIdPart());
      if (useSingleTable) {
        paramInd++;
        statement.setString(paramInd, resource.getResourceType().name());
      }
      paramInd++;
      statement.setString(paramInd, parser.encodeResourceToString(resource));
      statement.execute();
      statement.close();
    }
  }
}
