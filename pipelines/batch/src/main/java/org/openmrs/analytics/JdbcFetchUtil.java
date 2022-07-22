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

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.openmrs.analytics.model.DatabaseConfiguration;
import org.openmrs.analytics.model.EventConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcFetchUtil {
	
	private static final Logger log = LoggerFactory.getLogger(JdbcFetchUtil.class);
	
	private JdbcConnectionUtil jdbcConnectionUtil;
	
	JdbcFetchUtil(JdbcConnectionUtil jdbcConnectionUtil) {
		this.jdbcConnectionUtil = jdbcConnectionUtil;
	}
	
	public static class FetchUuids extends PTransform<PCollection<KV<Integer, Integer>>, PCollection<String>> {
		
		private final String tableName;
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchUuids(String tableName, JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.tableName = tableName;
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<String> expand(PCollection<KV<Integer, Integer>> idRanges) {
			String tableId = this.tableName + "_id";
			return idRanges.apply(String.format("Read UUIDs from %s", this.tableName),
			    JdbcIO.<KV<Integer, Integer>, String> readAll().withDataSourceConfiguration(this.dataSourceConfig)
			            .withParameterSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, Integer>>() {
				            
				            @Override
				            public void setParameters(KV<Integer, Integer> element, PreparedStatement preparedStatement)
				                    throws SQLException {
					            preparedStatement.setInt(1, element.getKey());
					            preparedStatement.setInt(2, element.getValue());
				            }
			            })
			            .withQuery(
			                String.format("SELECT uuid FROM %s WHERE %s >= ? AND %s <= ?", this.tableName, tableId, tableId))
			            .withRowMapper(new JdbcIO.RowMapper<String>() {
				            
				            @Override
				            public String mapRow(ResultSet resultSet) throws SQLException {
					            return resultSet.getString("uuid");
				            }
			            }).withCoder(NullableCoder.of(StringUtf8Coder.of())));
		}
	}
	
	/**
	 * batch together request using batchSize and generate segment descriptors `_id?<uuid>,<uuid>..`
	 */
	public static class CreateSearchSegments extends PTransform<PCollection<String>, PCollection<SearchSegmentDescriptor>> {
		
		private final String baseBundleUrl;
		
		private final String resourceType;
		
		private final int batchSize;
		
		public CreateSearchSegments(String resourceType, String baseBundleUrl, int batchSize) {
			this.baseBundleUrl = baseBundleUrl;
			this.batchSize = batchSize;
			this.resourceType = resourceType;
		}
		
		@Override
		public PCollection<SearchSegmentDescriptor> expand(PCollection<String> resourceUuids) {
			return resourceUuids
			        // create KV required by GroupIntoBatches
			        .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
				        
				        @ProcessElement
				        public void processElement(@Element String element, OutputReceiver<KV<String, String>> r) {
					        if (element != null) {
						        r.output(KV.of(resourceType, element));
					        }
				        }
			        }))
			        .apply(String.format("GroupIntoBatches of %s", this.batchSize),
			            GroupIntoBatches.<String, String> ofSize(this.batchSize))
			        .apply("Generate Segments", ParDo.of(new DoFn<KV<String, Iterable<String>>, SearchSegmentDescriptor>() {
				        
				        @ProcessElement
				        public void processElement(@Element KV<String, Iterable<String>> element,
				                OutputReceiver<SearchSegmentDescriptor> r) {
					        List<String> uuids = new ArrayList<String>();
					        element.getValue().forEach(uuids::add);
					        r.output(SearchSegmentDescriptor
					                .create(String.format("%s?_id=%s", baseBundleUrl, String.join(",", uuids)), uuids.size() //please note that, setting count here has no effect.
					        ));
					        
				        }
			        }));
		}
	}
	
	/**
	 * Utilizes Beam JdbcIO to query for resources directly from FHIR (HAPI) server's database and
	 * returns a PCollection of Lists of String objects - each corresponding to a resource's payload
	 */
	public static class FetchRowsJdbcIo extends PTransform<PCollection<List<String>>, PCollection<List<String>>> {
		
		private final JdbcIO.DataSourceConfiguration dataSourceConfig;
		
		public FetchRowsJdbcIo(JdbcIO.DataSourceConfiguration dataSourceConfig) {
			this.dataSourceConfig = dataSourceConfig;
		}
		
		@Override
		public PCollection<List<String>> expand(PCollection<List<String>> queryParameters) {
			return queryParameters.apply("JdbcIO readAll",
			    JdbcIO.<List<String>, List<String>> readAll().withDataSourceConfiguration(dataSourceConfig)
			            .withCoder(ListCoder.of(StringUtf8Coder.of()))
			            .withParameterSetter(new JdbcIO.PreparedStatementSetter<List<String>>() {
				            
				            @Override
				            public void setParameters(List<String> element, PreparedStatement preparedStatement)
				                    throws Exception {
					            preparedStatement.setString(1, element.get(0));
					            preparedStatement.setInt(2, Integer.valueOf(element.get(1)));
					            preparedStatement.setInt(3, Integer.valueOf(element.get(2)));
				            }
			            }).withOutputParallelization(true)
			            .withQuery(
			                "SELECT res.res_id, res.res_type, res.res_updated, res.res_ver, ver.res_encoding, ver.res_text FROM hfj_resource res, hfj_res_ver ver WHERE res.res_type=? AND res.res_id = ver.res_id AND res.res_id %? = ?")
			            .withRowMapper((JdbcIO.RowMapper<List<String>>) resultSet -> {
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
				            return Arrays.asList(resourceId, resourceType, resourceVersion, lastUpdated, jsonResource);
			            }));
			
		}
	}
	
	public PCollection<String> fetchUuidsByDate(Pipeline pipeline, String tableName, String dateColumn, String activePeriod)
	        throws SQLException, CannotProvideCoderException {
		Preconditions.checkArgument(!activePeriod.isEmpty());
		String[] dateRange = activePeriod.split("_");
		Preconditions.checkArgument(dateRange.length > 0);
		String lowerConstraint = "";
		if (!dateRange[0].isEmpty()) {
			lowerConstraint = String.format("%s > '%s'", dateColumn, dateRange[0]);
		}
		String upperConstraint = "";
		if (dateRange.length > 1 && !dateRange[1].isEmpty()) {
			upperConstraint = String.format("%s <= '%s'", dateColumn, dateRange[1]);
		}
		String constraint = "";
		if (!lowerConstraint.isEmpty() && !upperConstraint.isEmpty()) {
			constraint = String.format(" WHERE %s AND %s ", lowerConstraint, upperConstraint);
		} else {
			if (!lowerConstraint.isEmpty()) {
				constraint = String.format(" WHERE %s ", lowerConstraint);
			} else if (!upperConstraint.isEmpty()) {
				constraint = String.format(" WHERE %s ", upperConstraint);
			}
		}
		Statement statement = jdbcConnectionUtil.createStatement();
		ResultSet resultSet;
		final String query = String.format("SELECT uuid FROM %s %s", tableName, constraint);
		log.info("SQL query: " + query);
		resultSet = statement.executeQuery(query);
		List<String> uuids = Lists.newArrayList();
		while (resultSet.next()) {
			uuids.add(resultSet.getString("uuid"));
		}
		jdbcConnectionUtil.closeConnection(statement);
		log.info(String.format("Will fetch %d rows matching activePeriod in table %s", uuids.size(), tableName));
		// We need to specify the coder in case `uuids` is empty.
		return pipeline.apply(Create.of(uuids).withCoder(pipeline.getCoderRegistry().getCoder(String.class)));
	}
	
	public PCollection<String> fetchAllUuids(Pipeline pipeline, String tableName, int jdbcFetchSize) throws SQLException {
		int maxId = fetchMaxId(tableName);
		if (maxId == 0) {
			return pipeline.apply(Create.empty(StringUtf8Coder.of()));
		}
		log.info(String.format("Will fetch up to %d rows from table %s", maxId, tableName));
		Map<Integer, Integer> idRanges = createIdRanges(maxId, jdbcFetchSize);
		return pipeline.apply(Create.of(idRanges)).apply(new JdbcFetchUtil.FetchUuids(tableName, getJdbcConfig()));
	}
	
	private Integer fetchMaxId(String tableName) throws SQLException {
		String tableId = tableName + "_id";
		Statement statement = jdbcConnectionUtil.createStatement();
		ResultSet resultSet = statement
		        .executeQuery(String.format("SELECT MAX(`%s`) as max_id FROM %s", tableId, tableName));
		resultSet.first();
		int maxId = resultSet.getInt("max_id");
		jdbcConnectionUtil.closeConnection(statement);
		return maxId;
	}
	
	@VisibleForTesting
	JdbcIO.DataSourceConfiguration getJdbcConfig() {
		return JdbcIO.DataSourceConfiguration.create(this.jdbcConnectionUtil.getConnectionObject());
	}
	
	@VisibleForTesting
	Map<Integer, Integer> createIdRanges(int maxId, int rangeSize) {
		Map<Integer, Integer> rangeMap = new HashMap<Integer, Integer>();
		int ranges = maxId / rangeSize;
		for (int i = 0; i < ranges; i++) {
			int rangeFrom = (i * rangeSize) + 1;
			int rangeTo = (i + 1) * rangeSize;
			rangeMap.put(rangeFrom, rangeTo);
		}
		if (maxId > ranges * rangeSize) {
			int rangeFrom = (ranges * rangeSize) + 1;
			int rangeTo = ranges * rangeSize + maxId % rangeSize;
			rangeMap.put(rangeFrom, rangeTo);
		}
		return rangeMap;
	}
	
	/**
	 * Creates a map from database table names to the list of FHIR resources that correspond to that
	 * table. For example: person->Person,Patient and visit->Encounter.
	 *
	 * @param resourceString the comma separated list of FHIR resources we care about.
	 * @param dbConfig the general DB configuration.
	 * @return the computed map.
	 */
	public Map<String, List<String>> createFhirReverseMap(String resourceString, DatabaseConfiguration dbConfig) {
		Map<String, EventConfiguration> tableToFhirMap = dbConfig.getEventConfigurations();
		String[] resourceList = resourceString.split(",");
		Map<String, List<String>> reverseMap = new HashMap<String, List<String>>();
		for (Map.Entry<String, EventConfiguration> entry : tableToFhirMap.entrySet()) {
			Map<String, String> linkTemplate = entry.getValue().getLinkTemplates();
			for (String resource : resourceList) {
				if (linkTemplate.containsKey("fhir") && linkTemplate.get("fhir") != null) {
					String[] resourceName = linkTemplate.get("fhir").split("/");
					if (resourceName.length >= 1 && resourceName[1].equals(resource)) {
						if (reverseMap.containsKey(entry.getValue().getParentTable())) {
							List<String> resources = reverseMap.get(entry.getValue().getParentTable());
							resources.add(resourceName[1]);
						} else {
							List<String> resources = new ArrayList<String>();
							resources.add(resourceName[1]);
							reverseMap.put(entry.getValue().getParentTable(), resources);
						}
					}
				}
			}
		}
		if (reverseMap.size() < resourceList.length) {
			log.error("Some of the passed FHIR resources are not mapped to any table, please check the config");
		}
		return reverseMap;
	}
	
	/**
	 * Generates the query parameters by evenly distributing the workload to each query. The number of
	 * resources of each resource type is taken into consideration with respect to the total number of
	 * resources to be fetched inorder to determine the porportion of JdbcIo workers allocated.
	 */
	@VisibleForTesting
	List<List<String>> generateQueryParameters(Pipeline pipeline, String resourceList,
	        HashMap<String, Integer> resourceCount, int jdbcMaxPoolSize) {
		List<List<String>> queryParameterList = new ArrayList<List<String>>();
		
		int totalCount = 0;
		for (int count : resourceCount.values()) {
			totalCount += count;
		}
		
		for (String resourceType : resourceList.split(",")) {
			int numProcsAllocated = (int) Math.ceil((double) resourceCount.get(resourceType) / totalCount * jdbcMaxPoolSize);
			System.out.println(resourceType);
			System.out.println(numProcsAllocated);
			for (int i = 0; i < numProcsAllocated; i++) {
				queryParameterList.add(Arrays.asList(resourceType, String.valueOf(numProcsAllocated), String.valueOf(i)));
			}
		}
		return queryParameterList;
	}
	
	PCollection<List<String>> runJdbcIoFetch(Pipeline pipeline, String resourceList, HashMap<String, Integer> resourceCount,
	        int jdbcMaxPoolSize) {
		PCollection<List<String>> queryParameters = pipeline.apply("Create mod collection",
		    Create.of(generateQueryParameters(pipeline, resourceList, resourceCount, jdbcMaxPoolSize)));
		return queryParameters.apply(new JdbcFetchUtil.FetchRowsJdbcIo(getJdbcConfig()));
	}
}
