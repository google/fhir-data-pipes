/*
 * Copyright 2020-2024 Google LLC
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
package com.google.fhir.analytics.converter;

import ca.uhn.fhir.model.primitive.DateTimeDt;
import ca.uhn.fhir.model.primitive.InstantDt;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.Date;

public class JsonDateCodec implements JsonSerializer<Date>, JsonDeserializer<Date> {

  @Override
  public JsonElement serialize(
      Date date, Type type, JsonSerializationContext jsonSerializationContext) {
    if (date != null) {
      return new JsonPrimitive(new InstantDt(date).getValueAsString());
    }
    return null;
  }

  @Override
  public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    if (json != null && !json.isJsonNull()) {
      if (!json.isJsonPrimitive() || !json.getAsJsonPrimitive().isString()) {
        throw new JsonParseException("Cannot deserialize as json element is not a string");
      }
      String dateAsString = json.getAsJsonPrimitive().getAsString();
      return new DateTimeDt(dateAsString).getValue();
    }
    return null;
  }
}
