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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFetchHapi {

  private static final Logger log = LoggerFactory.getLogger(JdbcFetchHapi.class);

  private JdbcConnectionUtil jdbcConnectionUtil;

  JdbcFetchHapi(JdbcConnectionUtil jdbcConnectionUtil) {
    this.jdbcConnectionUtil = jdbcConnectionUtil;
  }

  /**
   * RowMapper class implementation for JdbcIo direct fetch with HAPI as the source FHIR server.
   * Each element in the ResultSet returned by the query maps to a List of String objects
   * corresponding to the column values in the query result.
   */
  public static class ResultSetToRowDescriptor implements JdbcIO.RowMapper<HapiRowDescriptor> {

    @Override
    public HapiRowDescriptor mapRow(ResultSet resultSet) throws Exception {
      String jsonResource = "";

      switch (resultSet.getString("res_encoding")) {
        case "JSON":
          jsonResource = new String(resultSet.getBytes("res_text"), Charsets.UTF_8);
          break;
        case "JSONC":
          Blob blob = resultSet.getBlob("res_text");
          jsonResource = GZipUtil.decompress(blob.getBytes(1, (int) blob.length()));
          blob.free();
          break;
        case "DEL":
          break;
      }

      String resourceId = resultSet.getString("res_id");
      String resourceType = resultSet.getString("res_type");
      String lastUpdated = resultSet.getString("res_updated");
      String resourceVersion = resultSet.getString("res_ver");
      return HapiRowDescriptor.create(
          resourceId, resourceType, lastUpdated, resourceVersion, jsonResource);
    }
  }

  /**
   * Utilizes Beam JdbcIO to query for resources directly from FHIR (HAPI) server's database and
   * returns a PCollection of Lists of String objects - each corresponding to a resource's payload
   */
  public static class FetchRowsJdbcIo
      extends PTransform<PCollection<QueryParameterDescriptor>, PCollection<HapiRowDescriptor>> {

    private final JdbcIO.DataSourceConfiguration dataSourceConfig;

    private final String query;

    public FetchRowsJdbcIo(JdbcIO.DataSourceConfiguration dataSourceConfig, String since) {
      this.dataSourceConfig = dataSourceConfig;
      // Note the constraint on `res.res_ver` ensures we only pick the latest version.
      StringBuilder builder =
          new StringBuilder(
              "SELECT res.res_id, res.res_type, res.res_updated, res.res_ver, "
                  + "ver.res_encoding, ver.res_text FROM hfj_resource res, hfj_res_ver ver "
                  + "WHERE res.res_type=? AND res.res_id = ver.res_id AND "
                  + "  res.res_ver = ver.res_ver AND res.res_id % ? = ? ");
      // TODO do date sanity-checking on `since` (note this is partly done by HAPI client call).
      if (since != null && !since.isEmpty()) {
        builder.append(" AND res.res_updated > '").append(since).append("'");
      }
      query = builder.toString();
      log.info("JDBC query template for HAPI is " + query);
    }

    @Override
    public PCollection<HapiRowDescriptor> expand(
        PCollection<QueryParameterDescriptor> queryParameters) {
      return queryParameters.apply(
          "JdbcIO readAll",
          JdbcIO.<QueryParameterDescriptor, HapiRowDescriptor>readAll()
              .withDataSourceConfiguration(dataSourceConfig)
              .withParameterSetter(
                  new JdbcIO.PreparedStatementSetter<QueryParameterDescriptor>() {

                    @Override
                    public void setParameters(
                        QueryParameterDescriptor element, PreparedStatement preparedStatement)
                        throws Exception {
                      preparedStatement.setString(1, element.resourceType());
                      preparedStatement.setInt(2, element.numBatches());
                      preparedStatement.setInt(3, element.batchId());
                    }
                  })
              // We are disabling this parameter because by default, this parameter causes JdbcIO to
              // add a reshuffle transform after reading from the database. This breaks fusion
              // between the read and write operations, thus resulting in high memory overhead.
              // Disabling the below parameter results in optimal performance.
              .withOutputParallelization(false)
              .withQuery(query)
              .withRowMapper(new ResultSetToRowDescriptor()));
    }
  }

  JdbcIO.DataSourceConfiguration getJdbcConfig() {
    return JdbcIO.DataSourceConfiguration.create(this.jdbcConnectionUtil.getDataSource());
  }

  /**
   * Generates the query parameters for the JdbcIO fetch. The query parameters are generated based
   * on the batch size option set for the job.
   *
   * @param options the pipeline options
   * @param resourceType the resource type
   * @param numResources total number of resources of the given type
   * @return a list of query parameters in the form of lists of strings
   */
  @VisibleForTesting
  List<QueryParameterDescriptor> generateQueryParameters(
      FhirEtlOptions options, String resourceType, int numResources) {
    log.info("Generating query parameters for " + resourceType);

    int jdbcFetchSize = options.getJdbcFetchSize();
    List<QueryParameterDescriptor> queryParameterList = new ArrayList<QueryParameterDescriptor>();
    int numBatches = numResources / jdbcFetchSize;
    if (numResources % jdbcFetchSize != 0) {
      numBatches += 1;
    }

    for (int i = 0; i < numBatches; i++) {
      queryParameterList.add(QueryParameterDescriptor.create(resourceType, numBatches, i));
    }

    return queryParameterList;
  }
}
