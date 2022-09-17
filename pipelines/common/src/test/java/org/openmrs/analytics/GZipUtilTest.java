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

import static org.junit.Assert.assertEquals;

import ca.uhn.fhir.parser.DataFormatException;
import com.google.common.io.Resources;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GZipUtilTest {

  @Test
  public void testDecompress() throws IOException {
    String patientResource =
        Resources.toString(Resources.getResource("patient.json"), StandardCharsets.UTF_8);

    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
      IOUtils.write(patientResource, gzipOutputStream, "UTF-8");
      byteArrayOutputStream.close();
      gzipOutputStream.close();
      byte[] patientResourceBytes = byteArrayOutputStream.toByteArray();

      String decompressedResource = GZipUtil.decompress(patientResourceBytes);
      assertEquals(decompressedResource, patientResource);

    } catch (IOException exception) {
      throw new DataFormatException("Failed to compress resource string", exception);
    }
  }
}
