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

// pipeline config
import static org.openmrs.analytics.PipelineConfig.EVENTS_HANDLER_ROUTE;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.debezium.data.Envelope;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

public class FhirUriGeneratorTest extends CamelTestSupport {
	
	@EndpointInject("mock:result")
	protected MockEndpoint resultEndpoint;
	
	@Produce(EVENTS_HANDLER_ROUTE)
	protected ProducerTemplate eventsProducer;
	
	@Before
	public void before() {
		resultEndpoint.reset();
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		return new RouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				
				// set debeziumEventConfigPath
				Properties p = System.getProperties();
				p.put("fhir.debeziumEventConfigPath", "../utils/dbz_event_to_fhir_config.json");
				System.setProperties(p);
				
				// Inject FhirUriGenerator;
				from(EVENTS_HANDLER_ROUTE).process(new FhirUriGenerator()) // inject target processor here
				        .to("mock:result");
			}
		};
	}
	
	@Test
	public void shouldGenerateCorrectFHIRUriGivenKnownTableEvent() throws Exception {
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "uuid");
			}
		};
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
				put(DebeziumConstants.HEADER_SOURCE_METADATA, new HashMap<String, Object>() {
					
					{
						put("table", "person");
					}
				});
			}
		};
		
		// should maintain original headers and bodies for downstream consumption
		resultEndpoint.expectedBodiesReceived(expectedBody.toString());
		resultEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
		resultEndpoint.expectedMessageCount(1);
		
		// should create FHIR URI header i.e fhirResourceUri
		resultEndpoint.expectedHeaderReceived("fhirResourceUri", "/ws/fhir2/R4/Person/uuid");
		
		// send events
		eventsProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// Assert All
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:result"));
		resultEndpoint.assertIsSatisfied();
	}
	
	@Test // this test might fail in future once we implement for non-openmrs tables
	public void shouldNotGenerateFHIRUriGivenUnknownTableEvent() throws Exception {
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "uuid");
			}
		};
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
				put(DebeziumConstants.HEADER_SOURCE_METADATA, new HashMap<String, Object>() {
					
					{
						put("table", "xray");
					}
				});
			}
		};
		
		// should maintain original headers and bodies for downstream consumption
		resultEndpoint.expectedBodiesReceived(expectedBody.toString());
		resultEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
		resultEndpoint.expectedMessageCount(1);
		
		// should return NULL
		resultEndpoint.expectedHeaderReceived("fhirResourceUri", null);
		
		// send events
		eventsProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// Assert All
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:result"));
		resultEndpoint.assertIsSatisfied();
	}
	
	@Test
	public void shouldNotGenerateFHIRUriGivenNullSourceMetadata() throws Exception {
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "uuid");
			}
		};
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
			}
		};
		
		// should maintain original headers and bodies for downstream consumption
		resultEndpoint.expectedBodiesReceived(expectedBody.toString());
		resultEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
		resultEndpoint.expectedMessageCount(1);
		
		// should return NULL
		resultEndpoint.expectedHeaderReceived("fhirResourceUri", null);
		
		// send events
		eventsProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// Assert All
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:result"));
		resultEndpoint.assertIsSatisfied();
	}
	
	@Test
	public void shouldNotGenerateFHIRUriGivenNullEventMetadata() throws Exception {
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
			}
		};
		
		// should maintain original headers for downstream consumption
		resultEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
		resultEndpoint.expectedMessageCount(1);
		
		// should return NULL
		resultEndpoint.expectedHeaderReceived("fhirResourceUri", null);
		
		// send null events
		eventsProducer.sendBodyAndHeaders(null, expectedHeader);
		
		// Assert All
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:result"));
		resultEndpoint.assertIsSatisfied();
	}
	
	@Test
	public void shouldEnsureAllOpenMrsCoreTablesHaveBeenFhirMapped() throws Exception {
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "uuid");
			}
		};
		
		String tables[] = { "obs", "encounter", "location", "cohort", "person", "provider", "relationship", "patient",
		        "drug", "allergy", "order", "drug_order", "test_order", "visit", "program", "patient_program" };
		
		int i;
		for (i = 0; i < tables.length; i++) {
			
			final String table = tables[i];
			Map<String, Object> expectedHeader = new HashMap<String, Object>() {
				
				{
					put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
					put(DebeziumConstants.HEADER_SOURCE_METADATA, new HashMap<String, Object>() {
						
						{
							put("table", table);
						}
					});
				}
			};
			
			// send events
			eventsProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		}
		
		// expect tables.length messages
		resultEndpoint.expectedMessageCount(tables.length);
		
		// Assert All
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:result"));
		resultEndpoint.assertIsSatisfied();
	}
	
}
