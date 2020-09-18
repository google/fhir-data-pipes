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
import java.util.Map;


import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


 // A Debezium events to FHIR URI mapper
public class FhirUriGenerator implements Processor {

    private static final Logger log = LoggerFactory.getLogger(DebeziumListener.class);
    private static final String EXPECTED_BODY_FORMAT = "{\"fhirResourceUri\":%s}";

    public void process(Exchange exchange) throws Exception {
        final Map payload = exchange.getMessage().getBody(Map.class);
        final Map sourceMetadata = exchange.getMessage().getHeader(DebeziumConstants.HEADER_SOURCE_METADATA, Map.class);
        // for malformed event, return null
        if (sourceMetadata==null || payload==null) {
            exchange.getIn().setHeader("fhirResourceUri", null);
            return;
        }
        final String table = sourceMetadata.get("table").toString();
        log.info("Processing Table --> " +table );
        // TODO make this configurable e.g using utils/fhir2_atom_feed_config.json
        switch (table) {
            case "obs":
                exchange.getIn().setHeader("fhirResourceUri", "/Observation/"+payload.get("uuid").toString());
                break;
            case "encounter":
                exchange.getIn().setHeader("fhirResourceUri", "/Encounter/"+payload.get("uuid").toString());
                break;
            case "location":
                exchange.getIn().setHeader("fhirResourceUri", "/Location/"+payload.get("uuid").toString());
                break;
            case "cohort":
                exchange.getIn().setHeader("fhirResourceUri", "/Group/"+payload.get("uuid").toString());
                break;
            case "person":
                exchange.getIn().setHeader("fhirResourceUri", "/Person/"+payload.get("uuid").toString());
                break;
            case "provider":
                exchange.getIn().setHeader("fhirResourceUri", "/Provider/"+payload.get("uuid").toString());
                break;
            case "relationship":
                exchange.getIn().setHeader("fhirResourceUri", "/Relationship/"+payload.get("uuid").toString());
                break;
            case "patient":
                exchange.getIn().setHeader("fhirResourceUri", "/Patient/"+payload.get("uuid").toString());
                break;
            case "drug":
                exchange.getIn().setHeader("fhirResourceUri", "/Drug/"+payload.get("uuid").toString());
                break;
            case "allergy":
                exchange.getIn().setHeader("fhirResourceUri", "/AllergyIntolerance/"+payload.get("uuid").toString());
                break;
            case "order":
                exchange.getIn().setHeader("fhirResourceUri", "/Order/"+payload.get("uuid").toString());
                break;
            case "drug_order":
                exchange.getIn().setHeader("fhirResourceUri", "/MedicationRequest/"+payload.get("uuid").toString());
                break;
            case "test_order":
                exchange.getIn().setHeader("fhirResourceUri", "/ProcedureRequest/"+payload.get("uuid").toString());
                break;
            case "visit":
                exchange.getIn().setHeader("fhirResourceUri", "/Encounter/"+payload.get("uuid").toString());
                break;
            case "program":
                exchange.getIn().setHeader("fhirResourceUri", "/Program/"+payload.get("uuid").toString());
                break;
            case "patient_program":
                exchange.getIn().setHeader("fhirResourceUri", "/Programenrollment/"+payload.get("uuid").toString());
                break;
            default:
                // TODO Implement ALL FHIR classes
                exchange.getIn().setHeader("fhirResourceUri",null);
                log.trace("Unknown Data..."  +table);
        }
    }
}