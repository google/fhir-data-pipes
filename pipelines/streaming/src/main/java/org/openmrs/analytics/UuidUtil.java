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

package org.openmrs.analytics;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class UuidUtil {

  private final JdbcConnectionUtil jdbcConnectionUtil;

  private Statement stmt;

  private ResultSet rs;

  public UuidUtil(JdbcConnectionUtil jdbcConnectionUtil) {
    this.jdbcConnectionUtil = jdbcConnectionUtil;
  }

  public String getUuid(String table, String keyColumn, String keyValue) throws SQLException {
    try {
      stmt = jdbcConnectionUtil.createStatement();
      String uuidResultFromSql = null;
      String sql = String.format("SELECT uuid FROM %s WHERE %s = %s", table, keyColumn, keyValue);

      rs = stmt.executeQuery(sql);
      while (rs.next()) {
        uuidResultFromSql = rs.getString("uuid");
      }
      if (uuidResultFromSql == null) {
        throw new SQLException("Could not find the uuid in the DB");
      } else {
        return uuidResultFromSql;
      }
    } finally {
      jdbcConnectionUtil.closeConnection(stmt);
    }
  }
}
