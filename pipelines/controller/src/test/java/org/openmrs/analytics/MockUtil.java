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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;

public class MockUtil {

  public static void mockResponse(MockWebServer mockWebServer, String filePath) throws IOException {
    InputStream stream = ClassLoader.getSystemResourceAsStream(filePath);
    assert stream != null;
    mockWebServer.enqueue(
        new MockResponse()
            .setBody(IOUtils.toString(stream, StandardCharsets.UTF_8))
            .addHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType()));
  }
}
