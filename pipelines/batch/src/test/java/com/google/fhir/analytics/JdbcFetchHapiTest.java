/*
 * Copyright 2020-2023 Google LLC
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
package com.google.fhir.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.fhir.analytics.model.DatabaseConfiguration;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
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

@RunWith(MockitoJUnitRunner.class)
public class JdbcFetchHapiTest extends TestCase {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Mock private DataSource mockedDataSource;

  @Mock private ResultSet resultSet;

  private FhirEtlOptions options;

  private JdbcFetchHapi jdbcFetchHapi;

  private DatabaseConfiguration dbConfig;

  @Before
  public void setup() throws IOException, PropertyVetoException {
    String[] args = {"--jdbcModeHapi=true", "--jdbcMaxPoolSize=48", "--jdbcFetchSize=10000"};
    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FhirEtlOptions.class);
    dbConfig = DatabaseConfiguration.createConfigFromFile("../../utils/hapi-postgres-config.json");
    jdbcFetchHapi = new JdbcFetchHapi(mockedDataSource);
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
    Mockito.when(resultSet.getString("res_version")).thenReturn("R4");

    HapiRowDescriptor rowDescriptor =
        new JdbcFetchHapi.ResultSetToRowDescriptor(options.getResourceList()).mapRow(resultSet);

    assertNotNull(rowDescriptor);
    assertEquals(rowDescriptor.resourceId(), "101");
    assertEquals(rowDescriptor.resourceType(), "Encounter");
    assertEquals(rowDescriptor.resourceVersion(), "1");
    assertEquals(rowDescriptor.lastUpdated(), "2002-03-12 10:09:20");
    assertEquals(rowDescriptor.fhirVersion(), "R4");
    assertEquals(rowDescriptor.jsonResource(), "");
  }

  @Test
  public void testSearchResourceCounts() throws SQLException {
    String resourceList = "Patient,Encounter,Observation";
    String since = "2002-03-12T10:09:20.123456Z";

    Connection mockedConnection = Mockito.mock(Connection.class);
    PreparedStatement mockedPreparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet mockedResultSet = Mockito.mock(ResultSet.class);
    Mockito.when(mockedPreparedStatement.executeQuery()).thenReturn(mockedResultSet);
    Mockito.when(mockedResultSet.getInt("count")).thenReturn(100, 100, 100);
    Mockito.when(mockedDataSource.getConnection()).thenReturn(mockedConnection);
    Mockito.when(
            mockedConnection.prepareStatement(
                "SELECT count(*) as count FROM hfj_resource res where res.res_type = ? AND"
                    + " res.res_updated > '"
                    + since
                    + "'"))
        .thenReturn(mockedPreparedStatement);

    Map<String, Integer> resourceCountMap = jdbcFetchHapi.searchResourceCounts(resourceList, since);

    assertThat(resourceCountMap.get("Patient"), equalTo(100));
    assertThat(resourceCountMap.get("Encounter"), equalTo(100));
    assertThat(resourceCountMap.get("Observation"), equalTo(100));
  }
}
