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
import java.util.Properties;

import io.debezium.data.Envelope.Operation;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumListenerTest extends CamelTestSupport {
	
	private static final Logger log = LoggerFactory.getLogger(DebeziumListenerTest.class);
	
	FhirConverter fhirConverterMock;
	
	int processCount;
	
	@Produce("direct:debezium") // mock to the original dbz
	protected ProducerTemplate debeziumProducer;
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		// mock properties
		Properties p = System.getProperties();
		p.put("openmrs.serverUrl", "http://mockfire:8099");
		p.put("openmrs.fhirBaseEndpoint", "/openmrs");
		p.put("openmrs.username", "dummy");
		p.put("openmrs.password", "dummy");
		p.put("fhir.sinkPath", "projects/PROJECT/locations/LOCATION/datasets/DATASET/fhirStores/FHIRSTORENAME");
		p.put("fhir.debeziumEventConfigPath", "../utils/dbz_event_to_fhir_config.json");
		System.setProperties(p);
		
		// Using a simple mock object would be more work since we need to provide stubs for the superclasses too.
		fhirConverterMock = new FhirConverter() {
			
			@Override
			public void process(Exchange exchange) {
				processCount++;
			}
		};
		return new DebeziumListener() {
			
			@Override
			FhirConverter createFhirConverter(CamelContext camelContext) {
				return fhirConverterMock;
			}
		};
	}
	
	@Override
	@Before
	public void setUp() throws Exception {
		// bypass for the need for mysql debezium instances: should remove this during EndToEnd testing
		replaceRouteFromWith(DebeziumListener.DEBEZIUM_ROUTE_ID, "direct:debezium");
		processCount = 0;
		super.setUp();
	}
	
	@Test
	public void shouldEnsureAllPipelineHandlersAreInitialized() throws Exception {
		// Assert All - real and mocks
		assertNotNull(context.hasEndpoint("direct:debezium"));
	}
	
	@Test
	public void shouldGenerateAndSinkFhirResourcesGivenCreateEvent() throws Exception {
		Map<String, String> expectedBody = DebeziumTestUtil.genExpectedBody();
		Map<String, Object> expectedHeader = DebeziumTestUtil.genExpectedHeaders(Operation.CREATE, "encounter");
		
		// send events
		debeziumProducer.sendBodyAndHeaders(expectedBody, expectedHeader);
		
		// validate assertions
		assertEquals(1, processCount);
	}
}
