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

// pipeline config;
// pipeline config;
import static org.openmrs.analytics.PipelineConfig.EVENTS_HANDLER_ROUTE;
import static org.openmrs.analytics.PipelineConfig.FHIR_HANDLER_ROUTE;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.debezium.data.Envelope;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.component.debezium.DebeziumConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumListenerTest extends CamelTestSupport {
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumListenerTest.class);
	
	private static final Map mockFhirPayload = new HashMap() {
		
		{
			put("id", "mockedID");
			put("resourceType", "Encounter");
			put("status", "unknown");
		}
	};
	
	@Produce("direct:debezium") // mock to the original dbz
	protected ProducerTemplate debeziumProducer;
	
	@EndpointInject("mock:event")
	protected MockEndpoint eventEndpoint;
	
	@EndpointInject("mock:fhir")
	protected MockEndpoint fhirEndpoint;
	
	@EndpointInject("mock:sink")
	protected MockEndpoint sinkEndpoint;
	
	@Produce(FHIR_HANDLER_ROUTE)
	protected ProducerTemplate fhirProducer;
	
	@Produce(EVENTS_HANDLER_ROUTE)
	protected ProducerTemplate eventProducer;
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		// mock properties
		Properties p = System.getProperties();
		p.put("openmrs.serverUrl", "http://mockfire:8099");
		p.put("openmrs.fhirBaseEndpoint", "/openmrs/ws/fhir2/R4");
		p.put("openmrs.username", "dummy");
		p.put("openmrs.password", "dummy");
		p.put("cloud.gcpFhirStore", "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME");
		System.setProperties(p);
		
		return new DebeziumListener();
	}
	
	@Override
	@Before
	public void setUp() throws Exception {
		// bypass for the need for mysql debezium instances: should remove this during EndToEnd testing
		replaceRouteFromWith(DebeziumListener.class.getName() + ".MysqlDatabaseCDC", "direct:debezium");
		super.setUp();
	}
	
	@Before
	public void before() throws Exception {
		eventEndpoint.reset();
		fhirEndpoint.reset();
		sinkEndpoint.reset();
	}
	
	@Before
	public void mockEndpoints() throws Exception {
		AdviceWithRouteBuilder interceptor = new AdviceWithRouteBuilder() {
			
			@Override
			public void configure() throws Exception {
				
				// intercept HTTP and mock payload
				interceptSendToEndpoint("http://*").skipSendToOriginalEndpoint().process(new Processor() {
					
					@Override
					public void process(Exchange exchange) throws Exception {
						exchange.getIn().setBody(mockFhirPayload);
					}
				}).marshal().json().to("mock:fhir").to("mock:sink");
				
			}
		};
		String routeId = DebeziumListener.class.getName() + ".FhirHandlerRoute";
		context.adviceWith(context.getRouteDefinition(routeId), interceptor);
	}
	
	@Test
	public void shouldEnsureAllPipelineHandlersAreInitialized() throws Exception {
		
		// Assert All - real and mocks
		assertNotNull(context.hasEndpoint(FHIR_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint(EVENTS_HANDLER_ROUTE));
		assertNotNull(context.hasEndpoint("mock:event"));
		assertNotNull(context.hasEndpoint("mock:fhir"));
		assertNotNull(context.hasEndpoint("mock:sink"));
		assertNotNull(context.hasEndpoint("direct:debezium"));
	}
	
	@Test
	public void shouldGenerateAndSinkFhirResourcesGivenCreateEvent() throws Exception {
		
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "encounter_uuid");
				
			}
		};
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.CREATE.code());
				put(DebeziumConstants.HEADER_SOURCE_METADATA, new HashMap<String, Object>() {
					
					{
						put("table", "encounter");
					}
				});
			}
		};
		
		// send events
		debeziumProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// should maintain original headers and bodies for downstream consumption
		fhirEndpoint.expectedBodiesReceived(expectedBody.toString());
		fhirEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.CREATE.code());
		fhirEndpoint.expectedMessageCount(1);
		
		// should send correct payload to sink (local/cloud)
		sinkEndpoint.expectedBodiesReceived(mockFhirPayload.toString());
		sinkEndpoint.expectedMessageCount(1);
		
		// validate assertions
		fhirEndpoint.assertIsSatisfied();
		sinkEndpoint.assertIsSatisfied();
	}
	
	@Test
	public void shouldGenerateAndSinkFhirResourcesGivenUpdateEvent() throws Exception {
		
		// expected Body/SourceMetadata
		Map expectedBody = new HashMap() {
			
			{
				put("uuid", "encounter_uuid");
				
			}
		};
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
				put(DebeziumConstants.HEADER_SOURCE_METADATA, new HashMap<String, Object>() {
					
					{
						put("table", "encounter");
					}
				});
			}
		};
		
		// send events
		debeziumProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// should maintain original headers and bodies for downstream consumption
		fhirEndpoint.expectedBodiesReceived(expectedBody.toString());
		fhirEndpoint.expectedHeaderReceived(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.UPDATE.code());
		fhirEndpoint.expectedMessageCount(1);
		
		// should send correct payload to sink (local/cloud)
		sinkEndpoint.expectedBodiesReceived(mockFhirPayload.toString());
		sinkEndpoint.expectedMessageCount(1);
		
		// validate assertions
		fhirEndpoint.assertIsSatisfied();
		sinkEndpoint.assertIsSatisfied();
	}
	
	@Test // TODO We might need to handle manual deletion in future.
	public void shouldNotSinkFhirResourcesGivenDeleteEvent() throws Exception {
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, Envelope.Operation.DELETE.code());
			}
		};
		
		// send events
		eventProducer.sendBodyAndHeaders(null, expectedHeader);
		
		// Should not HIT FHIR endpoint
		fhirEndpoint.expectedMessageCount(0);
		
		// Neither should
		sinkEndpoint.expectedMessageCount(0);
		
		// validate assertions
		fhirEndpoint.assertIsSatisfied();
		sinkEndpoint.assertIsSatisfied();
	}
	
	@Test
	public void shouldNotSinkFhirResourcesGivenUnsupportedEvent() throws Exception {
		
		// expected Headers/EventMetadata
		Map<String, Object> expectedHeader = new HashMap<String, Object>() {
			
			{
				put(DebeziumConstants.HEADER_OPERATION, "x");
			}
		};
		
		// send events
		eventProducer.sendBodyAndHeaders(null, expectedHeader);
		
		// Should not HIT FHIR endpoint
		fhirEndpoint.expectedMessageCount(0);
		
		// Neither should
		sinkEndpoint.expectedMessageCount(0);
		
		// validate assertions
		fhirEndpoint.assertIsSatisfied();
		sinkEndpoint.assertIsSatisfied();
	}
	
}
