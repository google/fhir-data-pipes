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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

public class UuidUtil {

  private final DataSource jdbcSource;

  public UuidUtil(DataSource jdbcSource) {
    this.jdbcSource = jdbcSource;
  }

  public String getUuid(String table, String keyColumn, String keyValue) throws SQLException {
    String sql = String.format("SELECT uuid FROM %s WHERE %s = %s", table, keyColumn, keyValue);
    try (Connection connection = jdbcSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      String uuidResultFromSql = null;
      while (resultSet.next()) {
        uuidResultFromSql = resultSet.getString("uuid");
      }
      if (uuidResultFromSql == null) {
        throw new SQLException("Could not find the uuid in the DB");
      } else {
        return uuidResultFromSql;
      }
    }
  }
}
