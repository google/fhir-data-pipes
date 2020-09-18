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



// Pipeline Connector configurator e.g dbz
public class PipelineConfig {

    private static final String APP_NAME = "DBZ";
    public static final String FHIR_HANDLER_ROUTE = "direct:get-fhir";
    public static final String EVENTS_HANDLER_ROUTE = "direct:get-events";

    public static String getDebeziumConfig() {
        return "debezium-mysql:{{database.hostname}}?"
                + "databaseHostname={{database.hostname}}"
                + "&databaseServerId={{database.serverId}}"
                + "&databasePort={{database.port}}"
                + "&databaseUser={{database.user}}"
                + "&databasePassword={{database.password}}"
                //+ "&name={{database.dbname}}"
                + "&databaseServerName={{database.dbname}}"
                + "&databaseWhitelist={{database.schema}}"
                + "&offsetStorage=org.apache.kafka.connect.storage.FileOffsetBackingStore"
                + "&offsetStorageFileName={{database.offsetStorage}}"
                + "&databaseHistoryFileFilename={{database.databaseHistory}}"
                //+ "&tableWhitelist={{database.schema}}.encounter,{{database.schema}}.obs"
                ;
    }

    public static String getFhirConfig() {
        return "{{openmrs.serverUrl}}{{openmrs.fhirBaseEndpoint}}${header.fhirResourceUri}" +
                "?httpMethod=GET" +
                "&authMethod=Basic" +
                "&authUsername={{openmrs.username}}" +
                "&authPassword={{openmrs.password}}" +
                "&authenticationPreemptive=true"+
                "&_summary=data";
    }
}