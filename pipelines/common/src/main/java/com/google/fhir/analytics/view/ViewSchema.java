/*
 * Copyright 2020-2025 Google LLC
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.fhir.analytics.view.ViewApplicator.FlatRow;
import com.google.fhir.analytics.view.ViewApplicator.RowElement;
import com.google.fhir.analytics.view.ViewApplicator.RowList;
import com.google.fhir.analytics.view.ViewDefinition.Column;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.jspecify.annotations.Nullable;
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
   * @return the corresponding JDBCType; if `fhirType` is not mapped JDBCType.VARCHAR is returned.
   */
  public static JDBCType fhirTypeToDb(@Nullable String fhirType) {
    if (fhirType != null) {
      switch (fhirType) {
        case "boolean":
          return JDBCType.BOOLEAN;
        case "integer":
        case "unsignedInt":
          return JDBCType.INTEGER;
        case "integer64":
          return JDBCType.BIGINT;
        case "decimal":
          return JDBCType.DECIMAL;
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
    }
    // This is to handle non-primitive types or when the type is not specified, we may want to
    // separate these case from string in the future.
    return JDBCType.VARCHAR;
  }

  /**
   * Creates a DB schema map for a given view.
   *
   * @param view the input view
   * @return an ordered map from column names to DB types
   */
  public static ImmutableMap<String, JDBCType> getDbSchema(ViewDefinition view) {
    if (view.getAllColumns() == null) return ImmutableMap.of();

    ImmutableMap.Builder<String, JDBCType> builder = ImmutableMap.builder();
    for (Entry<String, Column> entry : view.getAllColumns().entrySet()) {
      // This is internally guaranteed.
      Preconditions.checkState(entry.getValue() != null);
      if (entry.getValue().getType() == null && entry.getValue().getInferredType() == null) {
        log.warn("No type specified for column {}; using string instead.", entry.getKey());
      }
      builder.put(entry.getKey(), fhirTypeToDb(entry.getValue().getType()));
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
      ind++;
      if (re.getPrimitive() != null) {
        // TODO add unit-tests for all cases and add extra cases too if needed!
        if (ViewApplicator.ID_TYPE.equals(re.getColumnInfo().getInferredType())) {
          statement.setString(ind, re.getString());
        } else {
          switch (fhirTypeToDb(re.getColumnInfo().getType())) {
            case BOOLEAN:
              statement.setBoolean(ind, re.getPrimitive());
              break;
            case INTEGER:
              statement.setInt(ind, re.getPrimitive());
              break;
            case BIGINT:
              statement.setLong(ind, re.getPrimitive());
              break;
            case DECIMAL:
              statement.setBigDecimal(ind, re.getPrimitive());
              break;
            case DATE:
            case TIMESTAMP:
            case TIME:
              statement.setTimestamp(ind, new Timestamp(re.<Date>getPrimitive().getTime()));
              break;
            case VARCHAR:
            default:
              statement.setString(ind, re.getString());
              break;
          }
        }
      } else {
        // Currently arrays in DB are not supported; also type inference is not enabled.
        if (re.getColumnInfo().isCollection()
            || fhirTypeToDb(re.getColumnInfo().getType()) == JDBCType.VARCHAR) {
          statement.setString(
              ind,
              re.getValues() == null
                  ? null
                  : String.join(
                      ",",
                      re.getValues().stream().map(v -> v.toString()).collect(Collectors.toList())));
        } else {
          // This happens when there is no value for a column with a primitive type.
          statement.setNull(ind, fhirTypeToDb(re.getColumnInfo().getType()).getVendorTypeNumber());
        }
      }
    }
  }

  /**
   * Loads flat view values into GenericRecords by adding each RowElement value to its respective
   * column in a GenericRecord. Each GenericRecord contains values for one FlatRow
   *
   * @param rows RowList containing materialized view data
   * @param vDef The current ViewDefinition used to define the flat view
   * @return List of populated GenericRecords
   */
  public static List<GenericRecord> setValueInRecord(RowList rows, ViewDefinition vDef) {
    List<GenericRecord> recordList = new ArrayList<>();

    // Add each RowElement value to its respective column in the GenericRecord
    Schema flatSchema = ViewSchema.getAvroSchema(vDef);
    for (FlatRow flatRow : rows.getRows()) {
      GenericRecord currentRecord = new GenericData.Record(flatSchema);
      for (RowElement e : flatRow.getElements()) {
        if (e.getPrimitive() != null) {
          if (ViewApplicator.ID_TYPE.equals(e.getColumnInfo().getInferredType())) {
            currentRecord.put(e.getColumnInfo().getName(), e.getString());
          } else {
            String elementType =
                e.getColumnInfo().getType() == null ? "any" : e.getColumnInfo().getType();
            switch (elementType) {
              case "boolean":
                currentRecord.put(e.getColumnInfo().getName(), e.<Boolean>getPrimitive());
                break;
              case "integer":
              case "unsignedInt":
              case "integer64":
              case "decimal":
                currentRecord.put(e.getColumnInfo().getName(), e.getPrimitive());
                break;
              case "date":
              case "dateTime":
              case "instant":
              case "time":
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
              default:
                currentRecord.put(e.getColumnInfo().getName(), e.getString());
                break;
            }
          }
        } else {
          if (e.getColumnInfo().isCollection() || e.getColumnInfo().getType() != null) {
            if (e.getValues() == null || e.getValues().isEmpty()) {
              currentRecord.put(e.getColumnInfo().getName(), null);
            } else {
              // Handles View Definition Collections and converts them to Avro String Arrays
              String[] values = new String[e.getValues().size()];
              for (int i = 0; i < e.getValues().size(); i++) {
                values[i] = e.getValues().get(i).toString();
              }
              currentRecord.put(e.getName(), values);
            }
          } else {
            // This happens when there is no value for a column with a primitive type.
            currentRecord.put(e.getColumnInfo().getName(), null);
          }
        }
      }
      recordList.add(currentRecord);
    }
    return recordList;
  }

  /**
   * Creates an Avro Schema for a given View Definition. Note: This conversion should be consistent
   * with
   *
   * @see com.cerner.bunsen.avro.converters.DefinitionToAvroVisitor
   * @param view the input View Definition
   * @return Avro Schema
   */
  public static Schema getAvroSchema(ViewDefinition view) {
    FieldAssembler<Schema> schemaFields =
        SchemaBuilder.record(view.getName()).namespace("org.viewDefinition").fields();

    if (view.getAllColumns() == null) return schemaFields.endRecord();

    for (Entry<String, Column> entry : view.getAllColumns().entrySet()) {
      Preconditions.checkState(entry.getValue() != null);
      String columnType = entry.getValue().getType();
      String colName = entry.getKey();

      if (columnType != null) {
        switch (columnType) {
          case "boolean" -> schemaFields.optionalBoolean(colName);
          case "integer", "unsignedInt" -> schemaFields.optionalInt(colName);
          case "integer64" -> schemaFields.optionalLong(colName);
          case "decimal" ->
          // We cannot use Avro logical type `decimal` to represent FHIR `decimal` type. The
          // reason is that
          // Avro `decimal` type  has a fixed scale and a maximum precision but with a fixed scale
          // we have
          // no guarantees on the precision of the FHIR `decimal` type. See this for more details:
          // https://github.com/GoogleCloudPlatform/openmrs-fhir-analytics/issues/156#issuecomment-880964207

          // Note: The use Avro primitive type `Double` decreases precision but allows for
          // conversion
          schemaFields.optionalDouble(colName);
          case "date",
              "dateTime",
              "instant",
              "time",
              "base64Binary",
              "canonical",
              "code",
              "id",
              "markdown",
              "oid",
              "string",
              "uri",
              "url",
              "uuid" -> schemaFields.optionalString(colName);
          default -> {
            log.error(
                "Unknown type {} for column {} of ViewDefinition {}; using string!",
                columnType,
                colName,
                view.getName());
            schemaFields.optionalString(colName);
          }
        }
      } else {
        if (entry.getValue().isCollection()) {
          schemaFields
              .name(colName)
              .type()
              .nullable()
              .array()
              .items(Schema.create(Schema.Type.STRING))
              .noDefault();
        } else {
          // This is to handle non-primitive types or when the type is not specified, we may want to
          // separate these cases from string in the future.
          schemaFields.optionalString(colName);
        }
      }
    }
    return schemaFields.endRecord();
  }
}
