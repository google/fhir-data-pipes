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

import io.debezium.data.Envelope.Operation;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Resource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FhirConverterTest extends CamelTestSupport {

  private static final String TEST_ROUTE = "direct:test";

  private static final String TEST_RESOURCE = "test FHIR resource";

  private static final String TEST_ID = "ID";

  private static final String TEST_UUID = "UUID";

  @Produce(TEST_ROUTE)
  protected ProducerTemplate eventsProducer;

  @Mock private OpenmrsUtil openmrsUtil;

  @Mock private FhirStoreUtil fhirStoreUtil;

  @Mock private Resource resource;

  @Mock private UuidUtil uuidUtil;

  @Mock private ParquetUtil parquetUtil;

  @Mock private StatusServer statusServer;

  private FhirConverter fhirConverter;

  @Override
  protected RoutesBuilder createRouteBuilder() {
    return new RouteBuilder() {

      @Override
      public void configure() throws Exception {

        String fhirDebeziumEventConfigPath = "../../utils/dbz_event_to_fhir_config.json";
        fhirConverter =
            new FhirConverter(
                openmrsUtil,
                fhirStoreUtil,
                parquetUtil,
                fhirDebeziumEventConfigPath,
                uuidUtil,
                statusServer);

        // Inject FhirUriGenerator;
        from(TEST_ROUTE).process(fhirConverter); // inject target processor here
      }
    };
  }

  @Test
  public void shouldFetchFhirResourceAndUploadToFhirStore() throws IOException {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, "encounter");
    resource = new Encounter();
    resource.setId(TEST_ID);
    Mockito.when(openmrsUtil.fetchFhirResource(Mockito.anyString())).thenReturn(resource);
    Mockito.when(fhirStoreUtil.getSinkUrl()).thenReturn("sinkPath");
    // empty parquet File Path
    Mockito.when(parquetUtil.getParquetPath()).thenReturn("");

    // Actual event that will trigger process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil).fetchFhirResource(Mockito.anyString());
    Mockito.verify(fhirStoreUtil).uploadResource(resource);
    Mockito.verify(parquetUtil, Mockito.never()).write(Mockito.<Resource>any());
  }

  @Test
  public void shouldGetUuidFromParent() throws SQLException {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBodyWithoutUUid();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.CREATE, "patient");

    Mockito.when(uuidUtil.getUuid("person", "person_id", "1")).thenReturn(TEST_UUID);

    // Actual event that will trigger process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil).fetchFhirResource("/Patient/UUID");
    Mockito.verify(uuidUtil).getUuid("person", "person_id", "1");
  }

  @Test
  public void shouldFetchFhirResourceAndOutputParquet() throws IOException {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, "encounter");
    resource = new Encounter();
    resource.setId(TEST_ID);
    final String testPath = "some_test_path";
    Mockito.when(openmrsUtil.fetchFhirResource(Mockito.anyString())).thenReturn(resource);
    Mockito.when(parquetUtil.getParquetPath()).thenReturn(testPath);
    // empty fhir sink path
    Mockito.when(fhirStoreUtil.getSinkUrl()).thenReturn("");

    // Actual event that will trigger process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil).fetchFhirResource(Mockito.anyString());
    Mockito.verify(fhirStoreUtil, Mockito.never()).uploadResource(Mockito.<Resource>any());
    Mockito.verify(parquetUtil, Mockito.times(1)).write(Mockito.<Resource>any());
  }

  @Test
  public void shouldFetchFhirResourceAndOutputParquetAndUploadToFhirSink() throws IOException {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, "encounter");
    resource = new Encounter();
    resource.setId(TEST_ID);
    final String testPath = "some_test_path";
    Mockito.when(openmrsUtil.fetchFhirResource(Mockito.anyString())).thenReturn(resource);
    Mockito.when(parquetUtil.getParquetPath()).thenReturn(testPath);
    Mockito.when(fhirStoreUtil.getSinkUrl()).thenReturn("someFhirSink");

    // Actual event that will trigger process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil).fetchFhirResource(Mockito.anyString());
    Mockito.verify(fhirStoreUtil).uploadResource(Mockito.<Resource>any());
    Mockito.verify(parquetUtil, Mockito.times(1)).write(Mockito.<Resource>any());
  }

  @Test
  public void shouldIgnoreDeleteEvent() {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.DELETE, "encounter");

    // Actual event that will tripper process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil, Mockito.times(0)).fetchFhirResource(Mockito.anyString());
  }

  @Test
  public void shouldIgnoreEventWithNoHeaders() throws Exception {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();

    // Actual event that will tripper process().
    eventsProducer.sendBody(messageBody);

    Mockito.verify(openmrsUtil, Mockito.times(0)).fetchFhirResource(Mockito.anyString());
  }

  @Test
  public void shouldIgnoreEventWithUnknownTable() {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    Map<String, Object> messageHeaders =
        DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, "dummy");

    // Actual event that will tripper process().
    eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);

    Mockito.verify(openmrsUtil, Mockito.times(0)).fetchFhirResource(Mockito.anyString());
  }

  @Test
  public void shouldGenerateFhirResourcesForTablesThatHaveBeenMappedInConfig() {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();

    String tables[] = {
      "obs",
      "encounter",
      "cohort",
      "person",
      "provider",
      "relationship",
      "patient",
      "drug",
      "allergy",
      "order",
      "drug_order",
      "test_order",
      "program"
    };

    for (String table : tables) {
      Map<String, Object> messageHeaders =
          DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, table);
      // send events
      eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);
    }

    Mockito.verify(openmrsUtil, Mockito.times(tables.length))
        .fetchFhirResource(Mockito.anyString());
  }

  @Test
  public void shouldNotFetchFhirResourcesForTablesWithNoCorrespondingFhirLinkTemplatesInConfig() {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    // these do not have have linkTemplates.fhir in config
    String tables[] = {"visittype", "patient_identifier", "person_attribute"};

    for (String table : tables) {

      Map<String, Object> messageHeaders =
          DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, table);

      // send events
      eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);
    }

    Mockito.verify(openmrsUtil, Mockito.times(0)).fetchFhirResource(Mockito.anyString());
  }

  @Test
  public void shouldNotFetchFhirResourcesForDisabledTablesInConfig() {
    Map<String, String> messageBody = DebeziumTestUtil.genExpectedBody();
    // visit, location has been disabled in config
    String tables[] = {"visit", "location"};

    for (String table : tables) {

      Map<String, Object> messageHeaders =
          DebeziumTestUtil.genExpectedHeaders(Operation.UPDATE, table);

      // send events
      eventsProducer.sendBodyAndHeaders(messageBody, messageHeaders);
    }

    Mockito.verify(openmrsUtil, Mockito.times(0)).fetchFhirResource(Mockito.anyString());
  }
}
