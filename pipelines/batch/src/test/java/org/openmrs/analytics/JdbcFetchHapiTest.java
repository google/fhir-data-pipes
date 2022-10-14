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

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import junit.framework.TestCase;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.openmrs.analytics.model.DatabaseConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class JdbcFetchHapiTest extends TestCase {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Mock private ResultSet resultSet;

  private FhirEtlOptions options;

  private JdbcFetchHapi jdbcFetchHapi;

  private DatabaseConfiguration dbConfig;

  @Before
  public void setup() throws IOException, PropertyVetoException {
    String[] args = {"--jdbcModeHapi=true", "--jdbcMaxPoolSize=48", "--jdbcFetchSize=10000"};
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    dbConfig = DatabaseConfiguration.createConfigFromFile("../../utils/hapi-postgres-config.json");
    JdbcConnectionUtil jdbcConnectionUtil =
        new JdbcConnectionUtil(
            options.getJdbcDriverClass(),
            dbConfig.makeJdbsUrlFromConfig(),
            dbConfig.getDatabaseUser(),
            dbConfig.getDatabasePassword(),
            options.getJdbcInitialPoolSize(),
            options.getJdbcMaxPoolSize());

    jdbcFetchHapi = new JdbcFetchHapi(jdbcConnectionUtil);
  }

  @Test
  public void testGenerateQueryParameters() throws Exception {
    String resourceType = "Observation";
    int resourceCount = 1000001;
    List<QueryParameterDescriptor> queryParameterList =
        jdbcFetchHapi.generateQueryParameters(options, resourceType, resourceCount);

    // Verify the total number of query parameters and the content of the query parameters is
    // correct
    assertEquals(queryParameterList.size(), 101);
    assertEquals(queryParameterList.get(0).resourceType(), "Observation");
    assertEquals(queryParameterList.get(0).numBatches(), 101);
    assertEquals(queryParameterList.get(0).batchId(), 0);
  }

  @Test
  public void testMapRow() throws Exception {
    Mockito.when(resultSet.getString("res_encoding")).thenReturn("DEL");
    Mockito.when(resultSet.getString("res_id")).thenReturn("101");
    Mockito.when(resultSet.getString("res_type")).thenReturn("Encounter");
    Mockito.when(resultSet.getString("res_updated")).thenReturn("2002-03-12 10:09:20");
    Mockito.when(resultSet.getString("res_ver")).thenReturn("1");

    HapiRowDescriptor rowDescriptor =
        new JdbcFetchHapi.ResultSetToRowDescriptor().mapRow(resultSet);

    assertNotNull(rowDescriptor);
    assertEquals(rowDescriptor.resourceId(), "101");
    assertEquals(rowDescriptor.resourceType(), "Encounter");
    assertEquals(rowDescriptor.resourceVersion(), "1");
    assertEquals(rowDescriptor.lastUpdated(), "2002-03-12 10:09:20");
    assertEquals(rowDescriptor.jsonResource(), "");
  }
}
