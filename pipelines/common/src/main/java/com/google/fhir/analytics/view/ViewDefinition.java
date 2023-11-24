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

import com.google.gson.Gson;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.Getter;

// TODO: Generate this class from StructureDefinition using tools like:
//  https://github.com/hapifhir/org.hl7.fhir.core/tree/master/org.hl7.fhir.core.generator
@Getter
public class ViewDefinition {

  private String name;
  private String resource;
  private String resourceVersion;
  private List<Select> select;
  private List<Where> where;
  private List<Constant> constant;

  public static ViewDefinition createFromFile(String jsonFile) throws IOException {
    Gson gson = new Gson();
    Path pathToFile = Paths.get(jsonFile);
    try (Reader reader = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
      return gson.fromJson(reader, ViewDefinition.class);
    }
  }

  public static ViewDefinition createFromString(String jsonContent) throws IOException {
    Gson gson = new Gson();
    return gson.fromJson(jsonContent, ViewDefinition.class);
  }

  public boolean validate() {
    // TODO: Add checks like resource type/version validation, etc.
    return true;
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

    private boolean validate() {
      // TODO: Check that exactly one field is non-null.
      return true;
    }
  }
}
