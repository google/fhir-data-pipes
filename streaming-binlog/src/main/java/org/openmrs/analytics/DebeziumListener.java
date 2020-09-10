// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.openmrs.analytics;

import io.debezium.data.Envelope;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


import static org.openmrs.analytics.PipelineConfig.getDebeziumConfig;

/**
 * Debezium change data capture / Listener
 */
public class DebeziumListener  extends RouteBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumListener.class);
    private static final String FHIR_ROUTE_HANDLER = "direct:get-fhir";

    // FhirStore
    private static final FhirStoreUtil FHIR_STORE_UTIL  = new FhirStoreUtil(System.getProperty("cloud.gcpFhirStore","gcp"));

    @Override
    public void configure() throws Exception {
        LOG.info("Debezium Listener Started... ");

        /**
         *  Event types definition: CRUD
         */
        final Predicate isCreateOrUpdateEvent =
                header(DebeziumConstants.HEADER_OPERATION).in(
                        constant(Envelope.Operation.READ.code()),
                        constant(Envelope.Operation.CREATE.code()),
                        constant(Envelope.Operation.UPDATE.code()));

        final Predicate isCreateEvent =
                header(DebeziumConstants.HEADER_OPERATION).in(
                        constant(Envelope.Operation.READ.code()),
                        constant(Envelope.Operation.CREATE.code()));

        final Predicate isUpdateEvent =
                header(DebeziumConstants.HEADER_OPERATION).in(
                        constant(Envelope.Operation.READ.code()),
                        constant(Envelope.Operation.UPDATE.code()));

        final Predicate isDeleteEvent =
                header(DebeziumConstants.HEADER_OPERATION).in(
                        constant(Envelope.Operation.READ.code()),
                        constant(Envelope.Operation.DELETE.code()));

        /**
         * Main Change Data Capture (DBZ) entrypoint
         * @param dbzConfig.
         */
        from(getDebeziumConfig())
                .routeId(DebeziumListener.class.getName() + ".MysqlDatabaseCDC")
                .log(LoggingLevel.INFO, "Incoming Events: ${body} with headers ${headers}")
                .choice()
                    .when(isCreateEvent)
                        .log(LoggingLevel.TRACE, "CreateEvent Emitted ---> ${body}")
                        .process(new FhirUriGenerator())
                        .to(FHIR_ROUTE_HANDLER)
                        .endChoice()
                    .when(isUpdateEvent)
                        .log(LoggingLevel.TRACE, "UpdateEvent Emitted ---> ${body}")
                        .process(new FhirUriGenerator())
                        .to(FHIR_ROUTE_HANDLER)
                        .endChoice()
                    .when(isDeleteEvent)
                        .log(LoggingLevel.TRACE, "DeleteEvent Emitted Not Supported ---> ${body}")
                        .endChoice()
                    .otherwise()
                        .log(LoggingLevel.WARN, "Event Not Supported: ${headers[" + DebeziumConstants.HEADER_IDENTIFIER + "]}")
                .endParent();

        /**
         * FHIR event worker
         * Current append mode is suppported i.e any Create or Update will be upserted to the warehouse
         * append mode should also work for deletion given that openmrs does not delete data instead voids
         * @param routeDefinition.
         */
        from(FHIR_ROUTE_HANDLER)
                .filter(body().isNotNull())// Filter non-Fhir uri
                .choice()
                    .when().simple("${header.fhirResourceUri} == null || ${header.fhirResourceUri} == ''")
                        .log(LoggingLevel.WARN,"FHIR URL Not Mapped --->  ${headers[" + DebeziumConstants.HEADER_IDENTIFIER + "]}")
                    .otherwise()
                        .setBody(simple("${null}")) // Set up body to null
                        .toD("{{openmrs.serverUrl}}{{openmrs.fhirBaseEndpoint}}${header.fhirResourceUri}" +
                                "?httpMethod=GET" +
                                "&authMethod=Basic" +
                                "&authUsername={{openmrs.username}}" +
                                "&authPassword={{openmrs.password}}" +
                                "&authenticationPreemptive=true")
                        .log(LoggingLevel.INFO, "FHIR GET Operation Completed ---> ${header.fhirResourceUri}")
                        .unmarshal().json(JsonLibrary.Jackson)
                        .log(LoggingLevel.TRACE, "unmarshalled FHIR ${body}")
                        // Send to cloud TODO implement sink to local
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                final Map kv = exchange.getMessage().getBody(Map.class);
                                String resourceType =kv.get("resourceType").toString();
                                String id =kv.get("id").toString();
                                String fhirJson  = exchange.getMessage().getBody(String.class);
                                LOG.info("Sinking FHIR to Cloud ----> "+kv.get("resourceType")+"/"+kv.get("id"));
                                FHIR_STORE_UTIL.uploadResourceToCloud(resourceType,id, fhirJson);
                            }
                        })
                    .endChoice();

    }
}

