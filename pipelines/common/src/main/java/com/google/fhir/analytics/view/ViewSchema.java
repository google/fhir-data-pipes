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
package com.google.fhir.analytics.view;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.fhir.analytics.view.ViewApplicator.RowElement;
import com.google.fhir.analytics.view.ViewDefinition.Column;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utility methods for conversion from FHIR types to DB types, writing FHIR primitive
 * objects into DB, etc.
 */
public class ViewSchema {
  private static final Logger log = LoggerFactory.getLogger(ViewSchema.class);

  // TODO add a FHIR version specific type validation to make sure all primitive types are covered.

  /**
   * Converts a fhir type to a JDBCType
   *
   * @param fhirType the given FHIR type
   * @return the corresponding JDBCType or JDBCType.NULL if `fhirType` is not mapped.
   */
  public static JDBCType fhirTypeToDb(String fhirType) {
    if (fhirType == null) {
      return JDBCType.NULL;
    }
    switch (fhirType) {
      case "boolean":
        return JDBCType.BOOLEAN;
      case "integer":
      case "unsignedInt":
        return JDBCType.INTEGER;
      case "integer64":
        return JDBCType.BIGINT;
      case "decimal":
        return JDBCType.DOUBLE;
      case "date":
        return JDBCType.DATE;
      case "dateTime":
      case "instant":
        return JDBCType.TIMESTAMP;
      case "time":
        return JDBCType.TIME;
      case "base64Binary":
      case "canonical":
      case "code":
      case "id":
      case "markdown":
      case "oid":
      case "string":
      case "uri":
      case "url":
      case "uuid":
        return JDBCType.VARCHAR;
    }
    return null;
  }

  /**
   * Creates a DB schema map for a given view.
   *
   * @param view the input view
   * @return an ordered map from column names to DB types
   */
  public static ImmutableMap<String, JDBCType> getDbSchema(ViewDefinition view) {
    ImmutableMap.Builder<String, JDBCType> builder = ImmutableMap.builder();
    for (Entry<String, Column> entry : view.getColumnTypes().entrySet()) {
      // This is internally guaranteed.
      Preconditions.checkState(entry.getValue() != null);
      JDBCType dbType = fhirTypeToDb(entry.getValue().getType());
      if (dbType != JDBCType.NULL) {
        builder.put(entry.getKey(), dbType);
      } else {
        log.warn(
            "No DB type mapping for column {} with type {}; using string instead.",
            entry.getKey(),
            entry.getValue());
        if (Strings.nullToEmpty(entry.getValue().getType()).isEmpty()) {
          // TODO once we fix column types derivation, this case should be changed to UNDEFINED.
          builder.put(entry.getKey(), JDBCType.VARCHAR);
          // builder.put(entry.getKey(), DbType.UNDEFINED);
        } else {
          // We may need special handling here for structures but for now use string.
          builder.put(entry.getKey(), JDBCType.VARCHAR);
        }
      }
    }
    return builder.build();
  }

  /**
   * Set the values of a given row into the `statement` based on the column information.
   *
   * @param rowElements the input elements of the row
   * @param statement the statement on which `set*()` methods are called to write the row elements
   * @throws SQLException
   */
  public static void setValueInStatement(
      ImmutableList<RowElement> rowElements, PreparedStatement statement) throws SQLException {
    int ind = 0;
    for (RowElement re : rowElements) {
      if (re.getPrimitive() != null) {
        // TODO add unit-tests for all cases and add extra cases too if needed!
        if (ViewApplicator.ID_TYPE.equals(re.getColumnInfo().getInferredType())) {
          statement.setString(++ind, re.getSingleIdPart());
        } else {
          switch (fhirTypeToDb(re.getColumnInfo().getType())) {
            case BOOLEAN:
              statement.setBoolean(++ind, re.getPrimitive());
              break;
            case INTEGER:
              statement.setInt(++ind, re.getPrimitive());
              break;
            case BIGINT:
              statement.setLong(++ind, re.getPrimitive());
              break;
            case DOUBLE:
              statement.setDouble(++ind, re.getPrimitive());
              break;
            case DATE:
            case TIMESTAMP:
            case TIME:
              statement.setTimestamp(++ind, new Timestamp(re.<Date>getPrimitive().getTime()));
              break;
            case VARCHAR:
            default:
              statement.setString(++ind, re.getString());
              break;
          }
        }
      } else {
        // Currently arrays in DB are not supported; also type inference is not enabled.
        if (re.getColumnInfo().isCollection()
            || (re.getColumnInfo().getType() == null
                && re.getColumnInfo().getInferredType() == null)) {
          statement.setString(
              ++ind,
              re.getValues() == null
                  ? null
                  : String.join(
                      ",",
                      re.getValues().stream().map(v -> v.toString()).collect(Collectors.toList())));
        } else {
          statement.setNull(
              ++ind, fhirTypeToDb(re.getColumnInfo().getType()).getVendorTypeNumber());
        }
      }
    }
  }
}
