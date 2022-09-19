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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.analytics.StatusServer.EventTimeHandler;

@RunWith(MockitoJUnitRunner.class)
public class StatusServerTest {

  private static final String TEST_VAR = "TEST_VAR";

  private static final String TEST_VALUE = "TEST_VALUE";

  private StatusServer.EventTimeHandler handler;

  @Mock HttpExchange exchange;

  @Mock Headers headers;

  ByteArrayOutputStream outputStream;

  @Before
  public void setUp() {
    handler = new EventTimeHandler();
    Mockito.when(exchange.getResponseHeaders()).thenReturn(headers);
    outputStream = new ByteArrayOutputStream();
    Mockito.when(exchange.getResponseBody()).thenReturn(outputStream);
  }

  @Test
  public void setVarAndRetrieve() throws IOException {
    handler.setVar(TEST_VAR, TEST_VALUE);
    Mockito.when(exchange.getRequestURI())
        .thenReturn(URI.create(StatusServer.PATH_PREFIX + "/" + TEST_VAR));

    handler.handle(exchange);

    Mockito.verify(exchange).getResponseHeaders();
    Mockito.verify(exchange).getResponseBody();
    assertThat(outputStream.toString(), equalTo(TEST_VALUE));
  }

  @Test
  public void retrieveUnsetVarShouldSucceedWithProperMessage() throws IOException {
    Mockito.when(exchange.getRequestURI())
        .thenReturn(URI.create(StatusServer.PATH_PREFIX + "/" + TEST_VAR));

    handler.handle(exchange);

    Mockito.verify(exchange).getResponseHeaders();
    Mockito.verify(exchange).getResponseBody();
    assertThat(outputStream.toString(), startsWith("Invalid variable " + TEST_VAR));
  }

  @Test
  public void baseUrlShouldSucceedWithProperMessage() throws IOException {
    Mockito.when(exchange.getRequestURI()).thenReturn(URI.create(StatusServer.PATH_PREFIX));

    handler.handle(exchange);

    Mockito.verify(exchange).getResponseHeaders();
    Mockito.verify(exchange).getResponseBody();
    assertThat(outputStream.toString(), startsWith("The path format "));
  }
}
