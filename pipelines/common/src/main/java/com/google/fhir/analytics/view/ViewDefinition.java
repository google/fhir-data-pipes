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
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Generate this class from StructureDefinition using tools like:
//  https://github.com/hapifhir/org.hl7.fhir.core/tree/master/org.hl7.fhir.core.generator
public class ViewDefinition {

  private static final Logger log = LoggerFactory.getLogger(ViewDefinition.class);
  private static Pattern CONSTANT_PATTERN = Pattern.compile("%[A-Za-z][A-Za-z0-9_]*");
  private static Pattern SQL_NAME_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z0-9_]*$");

  @Getter private String name;
  @Getter private String resource;
  @Getter private String resourceVersion;
  @Getter private List<Select> select;
  @Getter private List<Where> where;
  // We don't need to expose constants because we do the replacement as part of the setup.
  private List<Constant> constant;
  // This is also used internally for processing constants and should not be exposed.
  private final Map<String, String> constMap = new HashMap<>();

  // It is probably better to take schema generation and validation out of this class and leave
  // this to be a pure data-object. However, because this class is instantiated only with factory
  // methods, it is probably okay to keep the current pattern for now.
  private ImmutableMap<String, String> columnTypes; // Initialized once in `validateAndSetUp`.

  public ImmutableMap<String, DbType> getDbSchema() {
    ImmutableMap.Builder<String, DbType> builder = ImmutableMap.builder();
    for (Entry<String, String> entry : columnTypes.entrySet()) {
      // This is internally guaranteed.
      Preconditions.checkState(entry.getValue() != null);
      if (DB_TYPE_MAP.containsKey(entry.getValue())) {
        builder.put(entry.getKey(), DB_TYPE_MAP.get(entry.getValue()));
      } else {
        log.warn("No DB type mapping for column {} with type {}", entry.getKey(), entry.getValue());
        if (entry.getValue().isEmpty()) {
          // TODO once we fix column types derivation, this case should be changed to UNDEFINED.
          builder.put(entry.getKey(), DbType.STRING);
          // builder.put(entry.getKey(), DbType.UNDEFINED);
        } else {
          builder.put(entry.getKey(), DbType.NON_PRIMITIVE);
        }
      }
    }
    return builder.build();
  }

  // This class should only be instantiated with the `create*` factory methods.
  private ViewDefinition() {}

  public static ViewDefinition createFromFile(Path jsonFile)
      throws IOException, ViewDefinitionException {
    Gson gson = new Gson();
    try (Reader reader = Files.newBufferedReader(jsonFile, StandardCharsets.UTF_8)) {
      ViewDefinition view = gson.fromJson(reader, ViewDefinition.class);
      view.validateAndSetUp();
      return view;
    }
  }

  public static ViewDefinition createFromString(String jsonContent) throws ViewDefinitionException {
    Gson gson = new Gson();
    ViewDefinition view = gson.fromJson(jsonContent, ViewDefinition.class);
    view.validateAndSetUp();
    return view;
  }

  /**
   * This does two main tasks: 1) replacing constants in all FHIRPaths, 2) collecting the list of
   * column with their types and checking for inconsistencies.
   *
   * @throws ViewDefinitionException if there is any column inconsistency, e.g., duplicates.
   */
  private void validateAndSetUp() throws ViewDefinitionException {
    if (Strings.isNullOrEmpty(resource)) {
      throw new ViewDefinitionException(
          "The resource field of a view should be a valid FHIR resource type.");
    }
    if (constant != null) {
      for (Constant c : constant) {
        if (!SQL_NAME_PATTERN.matcher(c.name).matches()) {
          throw new ViewDefinitionException(
              "Constant name " + c.name + " does not match 'sql-name' pattern!");
        }
        constMap.put(c.getName(), c.convertValueToString());
      }
    }
    // We do the string replacements recursively here when constructing a ViewDefinition, such that
    // applying the FHIRPaths to many resources later on, does not need constant replacement.
    if (where != null) {
      for (Where w : where) {
        if (w.getPath() == null) {
          throw new ViewDefinitionException("The `path` of `where` cannot be null!");
        }
        w.path = validateAndReplaceConstants(w.getPath());
      }
    }
    columnTypes = ImmutableMap.copyOf(validateAndReplaceConstantsInSelects(select, newTypeMap()));
  }

  /**
   * @param selects the list of Select structures to be validated; the constant replacement happens
   *     in-place, i.e., inside Select structures.
   * @param currentColumns the set of column names already found in the parent view.
   * @return the [ordered] map of new column names and their types as string.
   * @throws ViewDefinitionException for repeated columns or other requirements not satisfied.
   */
  private TreeMap<String, String> validateAndReplaceConstantsInSelects(
      List<Select> selects, TreeMap<String, String> currentColumns) throws ViewDefinitionException {
    TreeMap<String, String> newCols = newTypeMap();
    if (selects == null) {
      return newCols;
    }
    for (Select s : selects) {
      newCols.putAll(
          validateAndReplaceConstantsInOneSelect(s, unionTypeMaps(currentColumns, newCols)));
    }
    return newCols;
  }

  private static TreeMap<String, String> newTypeMap() {
    return new TreeMap<>();
  }

  private static TreeMap<String, String> unionTypeMaps(
      TreeMap<String, String> m1, TreeMap<String, String> m2) {
    TreeMap<String, String> u = new TreeMap<>();
    u.putAll(m1);
    u.putAll(m2);
    return u;
  }

  private TreeMap<String, String> validateAndReplaceConstantsInOneSelect(
      Select select, TreeMap<String, String> currentColumns) throws ViewDefinitionException {
    TreeMap<String, String> newCols = newTypeMap();
    if (select.getColumn() != null) {
      for (Column c : select.getColumn()) {
        if (Strings.nullToEmpty(c.name).isEmpty()) {
          throw new ViewDefinitionException("Column name cannot be empty!");
        }
        if (!SQL_NAME_PATTERN.matcher(c.name).matches()) {
          throw new ViewDefinitionException(
              "Column name " + c.name + " does not match 'sql-name' pattern!");
        }
        if (Strings.nullToEmpty(c.path).isEmpty()) {
          throw new ViewDefinitionException("Column path cannot be empty for " + c.name);
        }
        if (currentColumns.containsKey(c.getName()) || newCols.containsKey(c.getName())) {
          throw new ViewDefinitionException("Repeated column name " + c.getName());
        }
        // TODO implement automatic type derivation support.
        newCols.put(c.getName(), Strings.nullToEmpty(c.getType()));
        c.path = validateAndReplaceConstants(c.getPath());
      }
    }
    if (!Strings.nullToEmpty(select.getForEach()).isEmpty()) {
      select.forEach = validateAndReplaceConstants(select.getForEach());
    }
    if (!Strings.nullToEmpty(select.getForEachOrNull()).isEmpty()) {
      select.forEachOrNull = validateAndReplaceConstants(select.getForEachOrNull());
    }
    newCols.putAll(
        validateAndReplaceConstantsInSelects(
            select.getSelect(), unionTypeMaps(currentColumns, newCols)));
    TreeMap<String, String> unionCols = null;
    if (select.getUnionAll() != null) {
      for (Select u : select.getUnionAll()) {
        TreeMap<String, String> uCols =
            validateAndReplaceConstantsInOneSelect(u, unionTypeMaps(currentColumns, newCols));
        if (unionCols == null) {
          unionCols = uCols;
        } else {
          if (!unionCols.equals(uCols)) {
            throw new ViewDefinitionException(
                "Union columns are not consistent "
                    + Arrays.toString(uCols.keySet().toArray())
                    + " vs "
                    + Arrays.toString(unionCols.keySet().toArray()));
          }
        }
      }
    }
    if (unionCols != null) {
      return unionTypeMaps(newCols, unionCols);
    }
    return newCols;
  }

  private String validateAndReplaceConstants(String fhirPath) throws ViewDefinitionException {
    Matcher matcher = CONSTANT_PATTERN.matcher(fhirPath);
    try {
      return matcher.replaceAll(
          m -> {
            String constName = m.group().substring(1); // drops the initial '%'.
            if (!constMap.containsKey(constName)) {
              // We throw an unchecked exception here because it is inside the lambda function.
              throw new IllegalArgumentException("Constant not defined: " + constName);
            }
            return constMap.get(constName);
          });
    } catch (IllegalArgumentException e) {
      // Here we catch that exception and throw the right checked exception.
      throw new ViewDefinitionException(e.getMessage());
    }
  }

  @Getter
  public static class Select {
    private List<Column> column;
    private List<Select> select;
    private String forEach;
    private String forEachOrNull;
    private List<Select> unionAll;
  }

  @Getter
  public static class Column {
    private String path;
    private String name;
    private String type;
  }

  @Getter
  public static class Where {
    private String path;
  }

  @Getter
  public static class Constant {
    private String name;
    private String valueBase64Binary;
    private Boolean valueBoolean;
    private String valueCanonical;
    private String valueCode;
    private String valueDate;
    private String valueDateTime;
    private String valueDecimal;
    private String valueId;
    private String valueInstant;
    private Integer valueInteger;
    private Integer valueInteger64;
    private String valueOid;
    private String valueString;
    private Integer valuePositiveInt;
    private String valueTime;
    private Integer valueUnsignedInt;
    private String valueUri;
    private String valueUrl;
    private String valueUuid;

    private String quoteString(String s) {
      return "'" + s + "'";
    }

    /**
     * @return a string that can replace this constant in FHIRPaths.
     * @throws ViewDefinitionException if zero or more than one value is defined.
     */
    public String convertValueToString() throws ViewDefinitionException {
      int c = 0;
      String stringValue = null;
      if (null != valueBase64Binary) {
        stringValue = quoteString(valueBase64Binary);
        c++;
      }
      if (null != valueCanonical) {
        stringValue = quoteString(valueCanonical);
        c++;
      }
      if (null != valueCode) {
        stringValue = quoteString(valueCode);
        c++;
      }
      if (null != valueDate) {
        stringValue = "@" + valueDate;
        c++;
      }
      if (null != valueDateTime) {
        stringValue = "@" + valueDateTime;
        c++;
      }
      if (null != valueDecimal) {
        stringValue = valueDecimal;
        c++;
      }
      if (null != valueId) {
        stringValue = quoteString(valueId);
        c++;
      }
      if (null != valueInstant) {
        stringValue = quoteString(valueInstant);
        c++;
      }
      if (null != valueOid) {
        stringValue = quoteString(valueOid);
        c++;
      }
      if (null != valueString) {
        stringValue = quoteString(valueString);
        c++;
      }
      if (null != valueTime) {
        stringValue = "@" + valueTime;
        c++;
      }
      if (null != valueUri) {
        stringValue = quoteString(valueUri);
        c++;
      }
      if (null != valueUrl) {
        stringValue = quoteString(valueUrl);
        c++;
      }
      if (null != valueUuid) {
        stringValue = quoteString(valueUuid);
        c++;
      }
      if (null != valueBoolean) {
        stringValue = valueBoolean.toString();
        c++;
      }
      if (null != valueUnsignedInt) {
        stringValue = valueUnsignedInt.toString();
        c++;
      }
      if (null != valuePositiveInt) {
        stringValue = valuePositiveInt.toString();
        c++;
      }
      if (null != valueInteger) {
        stringValue = valueInteger.toString();
        c++;
      }
      if (null != valueInteger64) {
        stringValue = valueInteger64.toString();
        c++;
      }
      if (stringValue == null) {
        throw new ViewDefinitionException("None of the value[x] elements are set!");
      }
      if (c > 1) {
        throw new ViewDefinitionException(
            "Exactly one the value[x] elements should be set; got " + c);
      }
      return stringValue;
    }
  }

  public enum DbType {
    BOOLEAN, // boolean
    INTEGER { // integer, positiveInt, unsignedInt
      @Override
      public String typeString() {
        return "INT";
      }
    },
    INTEGER64 { // integer64
      @Override
      public String typeString() {
        return "BIGINT";
      }
    },
    DOUBLE, // decimal
    DATE, // date
    DATETIME, // dateTime, instant
    TIME, // time
    // TODO differentiate between different string types of FHIR.
    STRING { // base64Binary, canonical, code, id, markdown, oid, string, uri, url, uuid
      @Override
      public String typeString() {
        return "VARCHAR(1048576)"; // 1024*1024
      }
    },
    NON_PRIMITIVE,
    UNDEFINED;

    public String typeString() {
      return this.name();
    }
  }

  // TODO add a FHIR version specific type validation to make sure all primitive types are covered.
  public static final ImmutableMap<String, DbType> DB_TYPE_MAP =
      ImmutableMap.<String, DbType>builder()
          .put("boolean", DbType.BOOLEAN)
          .put("integer", DbType.INTEGER)
          .put("unsignedInt", DbType.INTEGER)
          .put("integer64", DbType.INTEGER64)
          .put("decimal", DbType.DOUBLE)
          .put("date", DbType.DATE)
          .put("dateTime", DbType.DATETIME)
          .put("instant", DbType.DATETIME)
          .put("time", DbType.TIME)
          .put("base64Binary", DbType.STRING)
          .put("canonical", DbType.STRING)
          .put("code", DbType.STRING)
          .put("id", DbType.STRING)
          .put("markdown", DbType.STRING)
          .put("oid", DbType.STRING)
          .put("string", DbType.STRING)
          .put("uri", DbType.STRING)
          .put("url", DbType.STRING)
          .put("uuid", DbType.STRING)
          .build();
}
