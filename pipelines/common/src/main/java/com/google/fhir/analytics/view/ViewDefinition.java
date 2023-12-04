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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;

// TODO: Generate this class from StructureDefinition using tools like:
//  https://github.com/hapifhir/org.hl7.fhir.core/tree/master/org.hl7.fhir.core.generator
public class ViewDefinition {

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
  Map<String, String> constMap = new HashMap<>();

  public static ViewDefinition createFromFile(String jsonFile)
      throws IOException, ViewDefinitionException {
    Gson gson = new Gson();
    Path pathToFile = Paths.get(jsonFile);
    try (Reader reader = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
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
    validateAndReplaceConstantsInSelects(select, new HashSet<>());
  }

  /**
   * @param selects the list of Select structures to be validated; the constant replacement happens
   *     in-place, i.e., inside Select structures.
   * @param currentColumns the set of column names already found in the parent view.
   * @return the set of new column names.
   * @throws ViewDefinitionException for repeated columns or other requirements not satisfied.
   */
  private Set<String> validateAndReplaceConstantsInSelects(
      List<Select> selects, Set<String> currentColumns) throws ViewDefinitionException {
    Set<String> newCols = new HashSet<>();
    if (selects == null) {
      return newCols;
    }
    for (Select s : selects) {
      newCols.addAll(
          validateAndReplaceConstantsInOneSelect(s, Sets.union(currentColumns, newCols)));
    }
    return newCols;
  }

  private Set<String> validateAndReplaceConstantsInOneSelect(
      Select select, Set<String> currentColumns) throws ViewDefinitionException {
    Set<String> newCols = new HashSet<>();
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
        if (currentColumns.contains(c.getName()) || newCols.contains(c.getName())) {
          throw new ViewDefinitionException("Repeated column name " + c.getName());
        }
        newCols.add(c.getName());
        c.path = validateAndReplaceConstants(c.getPath());
      }
    }
    if (!Strings.nullToEmpty(select.getForEach()).isEmpty()) {
      select.forEach = validateAndReplaceConstants(select.getForEach());
    }
    if (!Strings.nullToEmpty(select.getForEachOrNull()).isEmpty()) {
      select.forEachOrNull = validateAndReplaceConstants(select.getForEachOrNull());
    }
    newCols.addAll(
        validateAndReplaceConstantsInSelects(
            select.getSelect(), Sets.union(currentColumns, newCols)));
    Set<String> unionCols = null;
    if (select.getUnionAll() != null) {
      for (Select u : select.getUnionAll()) {
        Set<String> uCols =
            validateAndReplaceConstantsInOneSelect(u, Sets.union(currentColumns, newCols));
        if (unionCols == null) {
          unionCols = uCols;
        } else {
          if (!unionCols.equals(uCols)) {
            throw new ViewDefinitionException(
                "Union columns are not consistent "
                    + Arrays.toString(uCols.toArray())
                    + " vs "
                    + Arrays.toString(unionCols.toArray()));
          }
        }
      }
    }
    if (unionCols != null) {
      return Sets.union(newCols, unionCols);
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
}
