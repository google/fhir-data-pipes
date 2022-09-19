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
import io.debezium.data.Envelope.Operation;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.hl7.fhir.r4.model.Resource;
import org.openmrs.analytics.model.DatabaseConfiguration;
import org.openmrs.analytics.model.EventConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A Debezium events to FHIR URI mapper
public class FhirConverter implements Processor {

  private static final Logger log = LoggerFactory.getLogger(FhirConverter.class);

  private final OpenmrsUtil openmrsUtil;

  private final FhirStoreUtil fhirStoreUtil;

  private final ParquetUtil parquetUtil;

  private final DatabaseConfiguration databaseConfiguration;

  private final UuidUtil uuidUtil;

  private final StatusServer statusServer;

  @VisibleForTesting
  FhirConverter() {
    this.openmrsUtil = null;
    this.fhirStoreUtil = null;
    this.parquetUtil = null;
    this.databaseConfiguration = null;
    this.uuidUtil = null;
    this.statusServer = null;
  }

  public FhirConverter(
      OpenmrsUtil openmrsUtil,
      FhirStoreUtil fhirStoreUtil,
      ParquetUtil parquetUtil,
      String configFileName,
      UuidUtil uuidUtil,
      StatusServer statusServer)
      throws IOException {
    // TODO add option for switching to Parquet-file outputs.
    this.openmrsUtil = openmrsUtil;
    this.fhirStoreUtil = fhirStoreUtil;
    this.parquetUtil = parquetUtil;
    this.databaseConfiguration = DatabaseConfiguration.createConfigFromFile(configFileName);
    this.uuidUtil = uuidUtil;
    this.statusServer = statusServer;
  }

  @VisibleForTesting
  void updateLastResourceTime(String fhirResource, Message message) {
    Long messageTime = message.getHeader(DebeziumConstants.HEADER_TIMESTAMP, Long.class);
    if (messageTime != null) {
      String messageTimeStr =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(messageTime), ZoneOffset.UTC)
              .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      statusServer.setVar("last_" + fhirResource, messageTimeStr);
    }
  }

  public void process(Exchange exchange) throws SQLException {
    Message message = exchange.getMessage();
    final Map payload = message.getBody(Map.class);
    final Map sourceMetadata =
        message.getHeader(DebeziumConstants.HEADER_SOURCE_METADATA, Map.class);
    String operation = message.getHeader(DebeziumConstants.HEADER_OPERATION, String.class);
    // for malformed event, return null
    if (sourceMetadata == null || payload == null || operation == null) {
      return;
    }
    if (!operation.equals(Operation.CREATE.code()) && !operation.equals(Operation.UPDATE.code())) {
      log.debug("Skipping non create/update operation " + message.getHeaders());
      return;
    }
    final String table = sourceMetadata.get("table").toString();
    log.debug("Processing Table --> " + table);
    final EventConfiguration config = databaseConfiguration.getEventConfigurations().get(table);

    if (config == null || !config.getLinkTemplates().containsKey("fhir")) {
      log.trace("Skipping unmapped data ..." + table);
      return;
    }
    if (!config.isEnabled()) {
      log.trace("Skipping disabled events ..." + table);
      return;
    }
    String uuid = "";
    if (payload.get("uuid") != null) {
      uuid = payload.get("uuid").toString();
    } else {
      if (config.getParentTable() == null) {
        log.error(String.format("No parentTable in %s ignoring payload %s ", table, payload));
        return;
      }
      uuid =
          uuidUtil.getUuid(
              config.getParentTable(),
              config.getParentForeignKey(),
              payload.get(config.getChildPrimaryKey()).toString());
    }
    final String fhirUrl = config.getLinkTemplates().get("fhir").replace("{uuid}", uuid);
    log.info("Fetching FHIR resource at " + fhirUrl);
    Resource resource = openmrsUtil.fetchFhirResource(fhirUrl);
    if (resource == null) {
      // TODO: check how this can be signalled to Camel to be retried.
      return;
    }
    final String fhirResource =
        config.getLinkTemplates().get("fhir").replace("/{uuid}", "").replaceAll("/", "");
    updateLastResourceTime(fhirResource, message);

    if (!parquetUtil.getParquetPath().isEmpty()) {
      try {
        parquetUtil.write(resource);
      } catch (IOException e) {
        log.error(String.format("Cannot create ParquetWriter Exception: %s", e));
      }
    }
    if (!fhirStoreUtil.getSinkUrl().isEmpty()) {
      fhirStoreUtil.uploadResource(resource);
    }
  }
}
