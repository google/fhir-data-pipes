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

import com.google.common.base.Preconditions;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcConnectionUtil {

  private static final Logger log = LoggerFactory.getLogger(JdbcConnectionUtil.class);

  private final ComboPooledDataSource comboPooledDataSource;

  JdbcConnectionUtil(
      String jdbcDriverClass,
      String jdbcUrl,
      String dbUser,
      String dbPassword,
      int initialPoolSize,
      int jdbcMaxPoolSize)
      throws PropertyVetoException {
    log.info(
        "Creating a JdbcConnectionUtil for "
            + jdbcUrl
            + " with driver class "
            + jdbcDriverClass
            + " and max pool size "
            + jdbcMaxPoolSize);
    Preconditions.checkArgument(
        initialPoolSize <= jdbcMaxPoolSize,
        "initialPoolSize cannot be larger than jdbcMaxPoolSize");
    comboPooledDataSource = new ComboPooledDataSource();
    comboPooledDataSource.setDriverClass(jdbcDriverClass);
    comboPooledDataSource.setJdbcUrl(jdbcUrl);
    comboPooledDataSource.setUser(dbUser);
    comboPooledDataSource.setPassword(dbPassword);
    comboPooledDataSource.setMaxPoolSize(jdbcMaxPoolSize);
    comboPooledDataSource.setInitialPoolSize(initialPoolSize);
    // Setting an idle time to reduce the number of connections when idle.
    comboPooledDataSource.setMaxIdleTime(30);
  }

  public Statement createStatement() throws SQLException {
    Connection con = getDataSource().getConnection();
    return con.createStatement();
  }

  public void closeConnection(Statement stmt) throws SQLException {
    if (stmt != null) {
      Connection con = stmt.getConnection();
      stmt.close();
      con.close();
    }
  }

  public ComboPooledDataSource getDataSource() {
    return comboPooledDataSource;
  }
}
