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
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to connect and validate jdbc connections */
public class JdbcConnectionUtil {

  private static final Logger logger = LoggerFactory.getLogger(JdbcConnectionUtil.class.getName());

  /**
   * This method connects to the jdbc with the given details and internally uses
   * connection.isValid() method to validate if the connection can be established.
   */
  static boolean validateJdbcDetails(
      String jdbcDriverClass, String jdbcUrl, String dbUser, String dbPassword)
      throws SQLException, ClassNotFoundException {
    logger.info("Validating jdbc connection :: {}, {}", jdbcDriverClass, jdbcUrl);
    Class.forName(jdbcDriverClass);
    try (Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {
      return connection.isValid(15);
    }
  }
}
