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
package org.openmrs.analytics;

import ca.uhn.fhir.parser.DataFormatException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GZipUtil {

  private static final Logger log = LoggerFactory.getLogger(GZipUtil.class);

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

  public static byte[] compress(String str) {
    if (str == null || str.length() == 0) {
      return new byte[0];
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream outputStream = null;
    try {
      outputStream = new GZIPOutputStream(out);
      outputStream.write(str.getBytes());
      outputStream.close();
      return out.toByteArray();
    } catch (IOException e) {
      log.error("Failed to compress string", e);
      throw new DataFormatException("Failed to compress string", e);
    }
  }
}
