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

import ca.uhn.fhir.parser.DataFormatException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;

public class GZipUtil {

  public static String decompress(byte[] theResource) {
    try {
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(theResource);
      GZIPInputStream inputStream = new GZIPInputStream(byteArrayInputStream);
      return IOUtils.toString(inputStream, "UTF-8");
    } catch (IOException exception) {
      exception.printStackTrace();
      throw new DataFormatException("Failed to decompress byte array", exception);
    }
  }
}
