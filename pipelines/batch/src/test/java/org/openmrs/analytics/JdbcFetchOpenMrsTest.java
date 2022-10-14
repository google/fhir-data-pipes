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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.Resources;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.io.FileUtils;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Resource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.analytics.model.DatabaseConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFetchOpenMrsTest extends TestCase {

  private String resourceStr;

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private Resource resource;

  private FhirContext fhirContext;

  private JdbcFetchOpenMrs jdbcFetchUtil;

  private ParquetUtil parquetUtil;

  private String basePath = "/tmp/JUNIT/Parquet/TEST/";

  private DatabaseConfiguration dbConfig;

  @Before
  public void setup() throws IOException, PropertyVetoException {
    URL url = Resources.getResource("encounter.json");
    resourceStr = Resources.toString(url, StandardCharsets.UTF_8);
    this.fhirContext = FhirContext.forR4Cached();
    IParser parser = fhirContext.newJsonParser();
    resource = parser.parseResource(Encounter.class, resourceStr);

    String[] args = {
      "--fhirSinkPath=", "--fhirServerUrl=http://localhost:8099/openmrs/ws/fhir2/R4"
    };
    FhirEtlOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    dbConfig =
        DatabaseConfiguration.createConfigFromFile("../../utils/dbz_event_to_fhir_config.json");

    JdbcConnectionUtil jdbcConnectionUtil =
        new JdbcConnectionUtil(
            options.getJdbcDriverClass(),
            dbConfig.makeJdbsUrlFromConfig(),
            dbConfig.getDatabaseUser(),
            dbConfig.getDatabasePassword(),
            options.getJdbcInitialPoolSize(),
            options.getJdbcMaxPoolSize());
    // TODO jdbcConnectionUtil should be replaced by a mocked JdbcConnectionUtil which does not
    // depend on options either, since we don't need real DB connections for unit-testing.
    jdbcFetchUtil = new JdbcFetchOpenMrs(jdbcConnectionUtil);
    parquetUtil = new ParquetUtil(fhirContext.getVersion().getVersion(), basePath);
    // clean up if folder exists
    File file = new File(basePath);
    if (file.exists()) FileUtils.cleanDirectory(file);
  }

  @Test
  public void testGetJdbcConfig() throws PropertyVetoException {
    JdbcIO.DataSourceConfiguration config = jdbcFetchUtil.getJdbcConfig();
    assertTrue(
        JdbcIO.PoolableDataSourceProvider.of(config).apply(null) instanceof PoolingDataSource);
  }

  @Test
  public void testCreateIdRanges() {
    int batchSize = 100;
    int maxId = 201;
    Map<Integer, Integer> idRanges = jdbcFetchUtil.createIdRanges(maxId, batchSize);
    Map<Integer, Integer> expectedMap = new HashMap<Integer, Integer>();
    expectedMap.put(201, 201);
    expectedMap.put(101, 200);
    expectedMap.put(1, 100);
    assertEquals(idRanges, expectedMap);
  }

  @Test
  public void testCreateSearchSegmentDescriptor() {

    String resourceType = "Encounter";
    String baseBundleUrl = "https://test.com/" + resourceType;
    int batchSize = 2;
    String[] uuIds = {"<uuid>", "<uuid>", "<uuid>", "<uuid>", "<uuid>", "<uuid>"};
    PCollection<SearchSegmentDescriptor> createdSegments =
        testPipeline
            .apply("Create input", Create.of(Arrays.asList(uuIds)))
            // Inject
            .apply(
                new JdbcFetchOpenMrs.CreateSearchSegments(resourceType, baseBundleUrl, batchSize));
    // create expected output
    List<SearchSegmentDescriptor> segments = new ArrayList<>();
    // first batch
    segments.add(
        SearchSegmentDescriptor.create(
            String.format(
                "%s?_id=%s", baseBundleUrl, String.join(",", new String[] {"<uuid>,<uuid>"})),
            2));
    // second batch
    segments.add(
        SearchSegmentDescriptor.create(
            String.format(
                "%s?_id=%s", baseBundleUrl, String.join(",", new String[] {"<uuid>,<uuid>"})),
            2));
    // third batch
    segments.add(
        SearchSegmentDescriptor.create(
            String.format(
                "%s?_id=%s", baseBundleUrl, String.join(",", new String[] {"<uuid>,<uuid>"})),
            2));
    // assert
    PAssert.that(createdSegments).containsInAnyOrder(segments);
    testPipeline.run();
  }

  @Test
  public void testCreateFhirReverseMap() throws Exception {
    Map<String, List<String>> reverseMap =
        jdbcFetchUtil.createFhirReverseMap("Patient,Person,Encounter,Observation", dbConfig);

    assertEquals(reverseMap.size(), 4);
    assertEquals(reverseMap.get("person").size(), 2);
    assertTrue(reverseMap.get("person").contains("Patient"));
    assertTrue(reverseMap.get("person").contains("Person"));
    assertTrue(reverseMap.get("encounter").contains("Encounter"));
    assertTrue(reverseMap.get("visit").contains("Encounter"));
    assertTrue(reverseMap.get("obs").contains("Observation"));
  }

  @Test
  public void testFetchAllUuidUtilonEmptyTable() throws SQLException, CannotProvideCoderException {
    JdbcFetchOpenMrs mockedJdbcFetchUtil = mock(JdbcFetchOpenMrs.class);
    JdbcConnectionUtil mockedJdbcConnectionUtil = mock(JdbcConnectionUtil.class);
    Statement mockedStatement = mock(Statement.class);
    ResultSet mockedResultSet = mock(ResultSet.class);
    mockedJdbcFetchUtil = new JdbcFetchOpenMrs(mockedJdbcConnectionUtil);

    when(mockedJdbcConnectionUtil.createStatement()).thenReturn(mockedStatement);
    when(mockedStatement.executeQuery("SELECT MAX(`obs_id`) as max_id FROM obs"))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.getInt("max_id")).thenReturn(0);

    PCollection<String> uuids = mockedJdbcFetchUtil.fetchAllUuids(testPipeline, "obs", 20);
    // pipeline should not fail on empty uuids
    PAssert.that(uuids).empty();
    testPipeline.run();
  }
}
