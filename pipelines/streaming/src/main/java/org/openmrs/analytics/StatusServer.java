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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple HTTP server to provide information about the pipeline status. This class is thread-safe.
 */
public class StatusServer {

  private static final Logger log = LoggerFactory.getLogger(StatusServer.class);

  static final String PATH_PREFIX = "/eventTime";

  private final EventTimeHandler handler;

  public StatusServer(int port) throws IOException {
    handler = new EventTimeHandler();
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(PATH_PREFIX, handler);
    server.start();
  }

  public void setVar(String name, String value) {
    handler.setVar(name, value);
  }

  @VisibleForTesting
  static class EventTimeHandler implements HttpHandler {

    private final Map<String, String> statusVars;

    public EventTimeHandler() {
      statusVars = Maps.newHashMap();
    }

    public synchronized void setVar(String name, String value) {
      statusVars.put(name, value);
    }

    private synchronized String getVar(String name) {
      if (statusVars.containsKey(name)) {
        return statusVars.get(name);
      }
      return null;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      log.info("Received request: " + exchange.getRequestURI());
      String var = exchange.getRequestURI().getPath().replaceFirst(PATH_PREFIX, "");
      String response = "";
      if (var.isEmpty() || !var.startsWith("/")) {
        StringBuilder builder =
            new StringBuilder(
                String.format(
                    "The path format should be %s/VAR_NAME for VAR_NAME in:\n", PATH_PREFIX));
        for (Entry<String, String> entry : statusVars.entrySet()) {
          builder.append(entry.getKey()).append("\n");
        }
        response = builder.toString();
      } else {
        String key = var.substring(1);
        response = getVar(key);
        if (response == null) {
          response = String.format("Invalid variable %s requested!", key);
        }
      }
      Headers headers = exchange.getResponseHeaders();
      headers.add("Content-Type", "text");
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(response.getBytes());
      }
    }
  }
}
